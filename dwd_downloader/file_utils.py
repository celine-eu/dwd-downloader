from pathlib import Path
import json
import requests
import tempfile
from typing import Dict, Any
from .utils import download_file
from .logger import get_logger
from .storage import get_storage

logger = get_logger(__name__)


def load_metadata(storage_cfg: dict, dataset_dir: Path) -> Dict[str, Any]:
    """
    Load metadata.json from storage backend (FS or S3).
    """
    storage = get_storage(storage_cfg)
    key = f"{dataset_dir}/metadata.json"

    if storage.exists(str(key)):
        try:
            local_path = storage.get_path(str(key))
            with open(local_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to read metadata.json from %s: %s", key, e)
    return {}


def save_metadata(
    storage_cfg: dict, dataset_dir: Path, metadata: Dict[str, Any]
) -> None:
    """
    Save metadata.json to the configured storage backend (FS or S3).
    """
    storage = get_storage(storage_cfg)
    key = f"{dataset_dir}/metadata.json"

    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
            json.dump(metadata, tmp, indent=2)
            tmp.flush()
            storage.save(tmp.name, str(key))
            logger.debug("Saved metadata.json -> %s", key)
    except Exception as e:
        logger.error("Failed to write metadata.json to %s: %s", key, e)


def download_with_fallback(
    storage_cfg: dict, url: str, dest: Path, decompress: bool = False
) -> bool:
    """
    Download a single file locally. Storage backend is provided for consistency but
    used only in template_fallback_download.
    """
    try:
        return download_file(url, dest, decompress=decompress)
    except requests.HTTPError as he:
        if he.response.status_code == 404:
            logger.debug("File not found (404): %s", url)
            return False
        raise
    except Exception as e:
        logger.error("Error downloading %s: %s", url, e)
        raise


def template_fallback_download(
    storage_cfg: dict, dataset: dict, date_str: str, run: str, var: str
):
    """
    Fallback download using the filename template.
    Downloads files locally and saves them to the configured storage (FS or S3).
    Stops after 3 consecutive 404s.
    """
    file_template = dataset["file_template"]
    steps = dataset["forecast_steps"]
    grid = dataset.get("grid", "")
    subgrid = dataset.get("subgrid", "")
    level = dataset.get("level", "")
    base_url = dataset["base_url"]
    decompress = storage_cfg.get("decompress", False)

    storage = get_storage(storage_cfg)
    data_dir = Path(storage_cfg.get("data_dir", "./data"))
    dataset_name = dataset["name"]

    missing_count = 0
    for step in steps:
        var_upper = var.upper() if dataset_name.startswith("icon-eu") else var
        filename = file_template.format(
            grid=grid,
            subgrid=subgrid,
            level=level,
            date=date_str,
            run=run,
            step=step,
            var=var,
            var_upper=var_upper,
        )

        dest = data_dir / dataset_name / run / var / filename
        url_file = f"{base_url}/{run}/{var}/{filename}"

        # download locally first
        success = download_with_fallback(
            storage_cfg, url_file, dest, decompress=decompress
        )
        if not success:
            missing_count += 1
            if missing_count >= 3:
                logger.warning(
                    "3 consecutive 404s reached, stopping fallback for run %s/%s",
                    run,
                    var,
                )
                break
            continue

        # then push into the configured storage backend
        key = f"{dataset_name}/{run}/{var}/{filename}"
        try:
            storage.save(str(dest), str(key))
            logger.debug("Uploaded %s to storage key %s", filename, key)
        except Exception as e:
            logger.error("Failed to save %s to storage: %s", key, e)
            raise
