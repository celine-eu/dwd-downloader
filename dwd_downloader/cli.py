import sys
import argparse
from datetime import datetime, timezone
from .api import dwd_downloader


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

    failed = dwd_downloader(args.config, args.date)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
