# api.py
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Tuple

from .mirror import IconDatasetMirror
from .logger import get_logger

from pathlib import Path
from typing import Tuple, Dict, Any
import yaml
import os

logger = get_logger(__name__)


class DwdIconDownloader:
    """
    Class-based orchestrator for DWD ICON dataset downloads.

    - Loads config from YAML (with env expansion).
    - Resolves CONFIG_PATH env overriding the config parameter.
    - Iterates datasets and triggers IconDatasetMirror for each.
    """

    def __init__(
        self,
        config_path: str | Path = "./config.yaml",
        date_str: str | None = None,
    ):

        # Resolve config path (CONFIG_PATH env has priority)
        env_config_path = os.getenv("CONFIG_PATH")
        if env_config_path:
            self.config_path = Path(env_config_path)
            logger.debug(
                "Using config path from env CONFIG_PATH at %s", self.config_path
            )
        else:
            self.config_path = Path(config_path)

        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

        self.date_str = date_str
        self.date = self._parse_date(date_str)

        self.datasets, self.storage_cfg = self.load_dataset_config(self.config_path)

    def load_dataset_config(
        self, path: Path
    ) -> Tuple[list[Dict[str, Any]], Dict[str, Any]]:
        """
        Load dataset config from YAML and expand environment variables.
        """

        raw_text = path.read_text(encoding="utf-8")
        expanded_text = os.path.expandvars(raw_text)
        cfg = yaml.safe_load(expanded_text)

        return cfg.get("datasets", []), cfg.get("storage", {})

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        try:
            return datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError as e:
            logger.error("Invalid date format: %s", e)
            raise

    def run(self, raise_exceptions: bool = False) -> bool:
        """
        Execute the download for all configured datasets.

        Returns:
            has_errors (bool): True if any dataset failed, False otherwise.
        """
        has_errors = False

        for dataset in self.datasets:
            ds_name = dataset.get("name") or "unknown"
            try:
                logger.debug("Starting mirror for dataset: %s", ds_name)
                # You can either call the class directly:
                IconDatasetMirror(dataset, self.storage_cfg, self.date).run()
                # or the compatibility function:
                # mirror_icon_dataset(dataset, self.storage_cfg, self.date)
            except Exception as e:
                logger.error(
                    "Failed to mirror dataset %s: %s",
                    ds_name,
                    e,
                    exc_info=True,
                )
                has_errors = True
                if raise_exceptions:
                    raise

        return has_errors


def dwd_downloader(
    config: str = "./config.yaml",
    date: str | None = None,
    raise_exceptions: bool = False,
) -> bool:
    """
    Backwards-compatible procedural API.

    Keeps the same signature and semantics as before so existing
    CLI and callers don't need to change.
    """
    try:
        runner = DwdIconDownloader(config_path=config, date_str=date)
        return runner.run(raise_exceptions=raise_exceptions)
    except Exception:
        if raise_exceptions:
            raise
        return True  # has_errors=True on failure
