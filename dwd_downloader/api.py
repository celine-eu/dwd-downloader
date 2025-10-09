from datetime import datetime, timezone
from pathlib import Path
from .datasets import load_dataset_config
from .mirror import mirror_icon_dataset


def dwd_downloader(config: str = "./config.yaml", date: str | None = None):

    if date is None or date == "":
        date = datetime.now(timezone.utc).strftime("%Y%m%d")

    datasets, storage_cfg = load_dataset_config(Path(config))
    date_ref = datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc)

    for dataset in datasets:
        mirror_icon_dataset(dataset, storage_cfg, date_ref)
