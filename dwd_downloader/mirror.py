# mirror.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

import json
import requests
from bs4 import BeautifulSoup

from .logger import get_logger
from .storage import get_storage, Storage
from .utils import download_to_storage

logger = get_logger(__name__)


class IconDatasetMirror:
    """
    Class-based ICON dataset mirror.

    - Incremental: uses metadata.json in storage to track already downloaded files.
    - Uses HTML index only to check file existence.
    - Streams data from HTTP -> Storage, with optional in-memory decompression.
    - Encapsulates metadata and HTML index logic.
    """

    def __init__(
        self,
        dataset: Dict[str, Any],
        storage_cfg: Dict[str, Any],
        date: datetime,
    ):
        self.dataset = dataset
        self.storage_cfg = storage_cfg
        self.date = date
        self.storage: Storage = get_storage(storage_cfg)

        self.dataset_name: str = dataset["name"]
        self.decompress: bool = bool(storage_cfg.get("decompress", False))
        self.base_url: str = dataset["base_url"]

        # where we keep incremental state
        self.metadata_key: str = self._metadata_key()
        self.metadata: Dict[str, Any] = self._load_metadata()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        now = datetime.now(timezone.utc)
        yyyymmdd = self.date.strftime("%Y%m%d")

        logger.info(
            "Starting mirror for dataset %s, date %s",
            self.dataset_name,
            yyyymmdd,
        )

        runs: List[str] = self.dataset["runs"]
        variables: List[str] = self.dataset["variables"]
        steps: List[int] = self.dataset["forecast_steps"]

        for run in runs:
            run_hour = int(run)
            run_dt = datetime(
                self.date.year,
                self.date.month,
                self.date.day,
                run_hour,
                tzinfo=timezone.utc,
            )
            if run_dt > now:
                logger.debug("Skipping future run %s%s", yyyymmdd, run)
                continue

            for var in variables:
                self.metadata.setdefault(var, {})

                available_files = self._get_available_files_from_html(
                    run=run,
                    var=var,
                    date_str=yyyymmdd,
                )
                if not available_files:
                    logger.warning(
                        "HTML index is empty or failed for '%s/%s'. Skipping",
                        run,
                        var,
                    )
                    continue

                for step in steps:
                    filename = self._build_filename(yyyymmdd, run, var, step)

                    # Skip if already recorded in metadata
                    if self._already_downloaded(var, filename):
                        continue

                    # Skip if not present on the remote HTML listing
                    if filename not in available_files:
                        logger.warning(
                            "File not found on server, skipping: %s", filename
                        )
                        continue

                    url = f"{self.base_url}/{run}/{var}/{filename}"
                    data_key = self._build_data_key(yyyymmdd, run, var, filename)
                    meta_key = self._build_meta_key(yyyymmdd, run, var, filename)

                    try:
                        success = download_to_storage(
                            url=url,
                            storage=self.storage,
                            data_key=data_key,
                            meta_key=meta_key,
                            decompress=self.decompress,
                        )
                        if success:
                            self._mark_downloaded(var, filename)
                            logger.info("Downloaded %s -> %s", filename, data_key)
                    except Exception as e:
                        logger.error(
                            "Failed downloading %s: %s", filename, e, exc_info=True
                        )

        self._save_metadata()
        logger.info("Completed mirror for dataset %s", self.dataset_name)

    # ------------------------------------------------------------------
    # Internal helpers: metadata
    # ------------------------------------------------------------------

    def _metadata_key(self) -> str:
        """
        Central place to define where metadata.json lives in storage.
        """
        return f"{self.dataset_name}/metadata.json"

    def _load_metadata(self) -> Dict[str, Any]:
        """
        Load metadata.json from storage backend (FS or S3).
        Returns {} if not present or unreadable.
        """
        if not self.storage.exists(self.metadata_key):
            return {}
        try:
            with self.storage.open(self.metadata_key, "rb") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(
                "Failed to read metadata.json from %s: %s", self.metadata_key, e
            )
            return {}

    def _save_metadata(self) -> None:
        """
        Save metadata.json to the configured storage backend (FS or S3) using streaming.
        """
        try:
            payload = json.dumps(self.metadata, indent=2).encode("utf-8")
            self.storage.write_stream(self.metadata_key, (payload,))
            logger.debug("Saved metadata.json -> %s", self.metadata_key)
        except Exception as e:
            logger.error(
                "Failed to write metadata.json to %s: %s", self.metadata_key, e
            )

    def _already_downloaded(self, var: str, filename: str) -> bool:
        return filename in self.metadata.get(var, {})

    def _mark_downloaded(self, var: str, filename: str) -> None:
        self.metadata.setdefault(var, {})
        self.metadata[var][filename] = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Internal helpers: HTML index
    # ------------------------------------------------------------------

    def _get_available_files_from_html(
        self,
        run: str,
        var: str,
        date_str: str,
    ) -> List[str]:
        """
        Scrape the HTML index of a DWD folder and return a list of files
        containing the given date string.
        This is now encapsulated inside the IconDatasetMirror.
        """
        folder_url = f"{self.base_url}/{run}/{var}/"
        try:
            resp = requests.get(folder_url, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            links = [
                str(a["href"])
                for a in soup.find_all("a")
                if str(a.get("href", "")).endswith(".grib2.bz2")
                and date_str in str(a.get("href", ""))
            ]
            logger.debug("Found %d files at %s", len(links), folder_url)
            return links
        except Exception as e:
            logger.warning("Failed to scrape HTML index %s: %s", folder_url, e)
            return []

    # ------------------------------------------------------------------
    # Internal helpers: filename & keys
    # ------------------------------------------------------------------

    def _build_filename(
        self,
        yyyymmdd: str,
        run: str,
        var: str,
        step: int,
    ) -> str:
        """
        Build the remote filename based on the dataset template.
        Supports {grid}, {subgrid}, {level}, {date}, {run}, {step}, {var}, {var_upper}.
        """
        template = self.dataset["file_template"]
        grid = self.dataset.get("grid", "")
        subgrid = self.dataset.get("subgrid", "")
        level = self.dataset.get("level", "")
        var_upper = var.upper()
        return template.format(
            grid=grid,
            subgrid=subgrid,
            level=level,
            date=yyyymmdd,
            run=run,
            step=step,
            var=var,
            var_upper=var_upper,
        )

    def _build_data_key(
        self,
        yyyymmdd: str,
        run: str,
        var: str,
        filename: str,
    ) -> str:
        """
        Logical storage key for the data file.

        If decompress=True and filename ends with .bz2, we strip .bz2 in the
        storage key to represent the decompressed file.
        """
        if self.decompress and filename.endswith(".bz2"):
            filename = filename[:-4]  # strip .bz2
        return f"{self.dataset_name}/{yyyymmdd}/{run}/{var}/{filename}"

    def _build_meta_key(
        self,
        yyyymmdd: str,
        run: str,
        var: str,
        filename: str,
    ) -> str:
        """
        Logical key for the JSON metadata sidecar for a single file.
        Always uses the same base as the data file but with '.json' suffix.
        """
        if self.decompress and filename.endswith(".bz2"):
            filename = filename[:-4]
        return f"{self.dataset_name}/{yyyymmdd}/{run}/{var}/{filename}.json"


def mirror_icon_dataset(
    dataset: Dict[str, Any],
    storage_cfg: Dict[str, Any],
    date: datetime,
) -> None:
    """
    Backwards-compatible wrapper to keep external call sites working.
    """
    IconDatasetMirror(dataset, storage_cfg, date).run()
