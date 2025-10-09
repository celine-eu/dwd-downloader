import os
from datetime import datetime, timezone
from pathlib import Path
from .datasets import load_dataset_config
from .mirror import mirror_icon_dataset
from .logger import get_logger
from dotenv import load_dotenv

logger = get_logger(__name__)


def dwd_downloader(
    config: str = "./config.yaml", date: str | None = None, raise_exceptions=False
) -> bool:

    load_dotenv()

    if date is None or date == "":
        date = datetime.now(timezone.utc).strftime("%Y%m%d")

    env_config_path = os.getenv("CONFIG_PATH")
    if env_config_path:
        config = env_config_path
        logger.debug("Using config path from env CONFIG_PATH at %s", config)

    try:
        datasets, storage_cfg = load_dataset_config(Path(config))
        logger.debug("Loaded config from %s", config)
    except Exception as e:
        logger.error("Failed to load config: %s", e, exc_info=True)
        if raise_exceptions:
            raise e
        return False

    try:
        date_ref = datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError as e:
        logger.error("Invalid date format: %s", e)
        if raise_exceptions:
            raise e
        return False

    has_errors = False
    for dataset in datasets:
        ds_name = dataset.get("name") or "unknown"
        try:
            logger.debug("Starting mirror for dataset: %s", ds_name)
            mirror_icon_dataset(dataset, storage_cfg, date_ref)
        except Exception as e:
            logger.error(
                "Failed to mirror dataset %s: %s",
                ds_name,
                e,
                exc_info=True,
            )
            has_errors = True
            if raise_exceptions:
                raise e

    return has_errors
