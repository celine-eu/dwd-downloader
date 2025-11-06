from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
from .html_utils import get_available_files_from_html
from .file_utils import download_file, load_metadata, save_metadata
from .logger import get_logger

logger = get_logger(__name__)


def mirror_icon_dataset(
    dataset: Dict[str, Any], storage_cfg: Dict[str, Any], date: datetime
) -> None:
    """
    Mirror one ICON dataset incrementally based on config.yaml.
    Only downloads files explicitly defined by file_template, runs, variables, and forecast_steps.
    HTML index is used only to check file existence.
    """
    now = datetime.now(timezone.utc)
    yyyymmdd = date.strftime("%Y%m%d")

    data_dir: Path = Path(storage_cfg.get("data_dir", "./data"))
    decompress: bool = bool(storage_cfg.get("decompress", False))
    dataset_dir: Path = data_dir / dataset["name"]
    metadata: Dict[str, Any] = load_metadata(storage_cfg, dataset_dir)

    logger.info("Starting mirror for dataset %s, date %s", dataset["name"], yyyymmdd)

    for run in dataset["runs"]:
        run_hour: int = int(run)
        run_dt = datetime(
            date.year, date.month, date.day, run_hour, tzinfo=timezone.utc
        )
        if run_dt > now:
            logger.debug("Skipping future run %s%s", yyyymmdd, run)
            continue

        for var in dataset["variables"]:
            metadata.setdefault(var, {})

            # Fetch available files from HTML index once per run/var
            url_folder: str = f"{dataset['base_url']}/{run}/{var}/"
            available_files: List[str] = get_available_files_from_html(
                url_folder, yyyymmdd
            )
            if not available_files:
                logger.warning(
                    "HTML index is empty or failed for '%s/%s'. Skipping", run, var
                )
                continue

            for step in dataset["forecast_steps"]:
                filename: str = dataset["file_template"].format(
                    grid=dataset.get("grid", ""),
                    subgrid=dataset.get("subgrid", ""),
                    level=dataset.get("level", ""),
                    date=yyyymmdd,
                    run=run,
                    step=step,
                    var=var,
                    var_upper=var.upper(),  # needed for icon-eu
                )

                # Skip if already downloaded
                if filename in metadata[var]:
                    continue

                # Skip if file not in HTML index
                if filename not in available_files:
                    logger.warning("File not found on server, skipping: %s", filename)
                    continue

                # Download
                dest: Path = dataset_dir / run / var / filename
                try:
                    success: bool = download_file(
                        url_folder + filename, dest, decompress=decompress
                    )
                    if success:
                        metadata[var][filename] = datetime.now(timezone.utc).isoformat()
                        logger.info("Downloaded: %s", dest)
                except Exception as e:
                    logger.error(
                        "Failed downloading %s: %s", filename, e, exc_info=True
                    )

    save_metadata(storage_cfg, dataset_dir, metadata)
    logger.info("Completed mirror for dataset %s", dataset["name"])
