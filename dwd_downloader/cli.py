import argparse
from datetime import datetime, timezone
from pathlib import Path
from .datasets import load_dataset_config
from .mirror import mirror_icon_dataset


def main():
    parser = argparse.ArgumentParser(description="DWD ICON Dataset Downloader")
    parser.add_argument(
        "--config",
        required=False,
        default="./config.yaml",
        help="Path to YAML configuration file",
    )

    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y%m%d"),
        help="Date in YYYYMMDD format",
    )
    args = parser.parse_args()

    datasets, storage_cfg = load_dataset_config(Path(args.config))
    date = datetime.strptime(args.date, "%Y%m%d").replace(tzinfo=timezone.utc)

    for dataset in datasets:
        mirror_icon_dataset(dataset, storage_cfg, date)


if __name__ == "__main__":
    main()
