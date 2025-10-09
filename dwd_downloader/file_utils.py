from pathlib import Path
from .utils import download_file
from .logger import get_logger
import requests
import json
from typing import Dict, Any

logger = get_logger(__name__)


def load_metadata(dataset_dir: Path) -> Dict[str, Any]:
    metadata_file = dataset_dir / "metadata.json"
    if metadata_file.exists():
        try:
            with metadata_file.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Failed to read metadata.json: %s", e)
    return {}


def save_metadata(dataset_dir: Path, metadata: Dict[str, Any]) -> None:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    metadata_file = dataset_dir / "metadata.json"
    try:
        with metadata_file.open("w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        logger.error("Failed to write metadata.json: %s", e)


def download_with_fallback(url: str, dest: Path, decompress: bool = False) -> bool:
    """
    Download a single file, return True if successful or already exists,
    False if 404, raises for other errors.
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
    dataset: dict, storage_cfg: dict, date_str: str, run: str, var: str
):
    """
    Fallback download using the filename template. Stops after 3 consecutive 404s.
    """
    file_template = dataset["file_template"]
    steps = dataset["forecast_steps"]
    grid = dataset.get("grid", "")
    subgrid = dataset.get("subgrid", "")
    level = dataset.get("level", "")
    base_url = dataset["base_url"]
    decompress = storage_cfg.get("decompress", False)
    data_dir = Path(storage_cfg.get("data_dir", "./data"))

    missing_count = 0
    for step in steps:
        var_upper = var.upper() if dataset["name"].startswith("icon-eu") else var
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
        dest = data_dir / dataset["name"] / run / var / filename
        url_file = f"{base_url}/{run}/{var}/{filename}"
        success = download_with_fallback(url_file, dest, decompress=decompress)
        if not success:
            missing_count += 1
            if missing_count >= 3:
                logger.warning(
                    "3 consecutive 404s reached, stopping fallback for run %s/%s",
                    run,
                    var,
                )
                break
