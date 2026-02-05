# mirror.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List
from datetime import timedelta

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

        self.cleanup_enabled = bool(storage_cfg.get("cleanup", False))
        self.keep_days = int(storage_cfg.get("keep_days", 1))
        self.keep_runs = int(storage_cfg.get("keep_runs", 1))

        self.download_latest_only = bool(storage_cfg.get("download_latest_only", True))

        # where we keep incremental state
        self.metadata_key: str = self._metadata_key()
        self.metadata: Dict[str, Any] = self._load_metadata()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _detect_newest_available_run_on_server(
        self,
        runs: List[str],
        date_str: str,
        variables: List[str],
    ) -> str | None:
        """
        Detect the newest run available on the DWD server by checking which runs
        have files available.

        Returns:
            The newest run hour (as string like "09", "12") or None if no runs found.
        """
        now = datetime.now(timezone.utc)

        # Filter to only past/current runs
        available_runs = []
        for run in runs:
            run_hour = int(run)
            run_dt = datetime(
                self.date.year,
                self.date.month,
                self.date.day,
                run_hour,
                tzinfo=timezone.utc,
            )
            if run_dt <= now:
                available_runs.append(run)

        if not available_runs:
            return None

        # Sort runs newest to oldest
        sorted_runs = sorted(available_runs, key=lambda r: int(r), reverse=True)

        # Check each run (starting with newest) to see if it has any files
        for run in sorted_runs:
            # Check first variable only (for efficiency)
            test_var = variables[0] if variables else None
            if not test_var:
                continue

            available_files = self._get_available_files_from_html(
                run=run,
                var=test_var,
                date_str=date_str,
            )

            if available_files:
                logger.info(
                    "Detected newest available run on server: %s (has %d files for %s)",
                    run,
                    len(available_files),
                    test_var,
                )
                return run

        logger.warning("No runs with available files found on server")
        return None

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

        existing_complete = self._detect_existing_complete_runs()

        newest_on_server = None
        if self.download_latest_only:
            newest_on_server = self._detect_newest_available_run_on_server(
                runs=runs,
                date_str=yyyymmdd,
                variables=variables,
            )
            if newest_on_server:
                logger.info(
                    "download_latest_only=True: Will only download run %s",
                    newest_on_server,
                )

        if self.keep_runs > 0 and existing_complete:
            # Sort by run hour descending
            existing_complete_sorted = sorted(
                existing_complete, key=lambda r: int(r), reverse=True
            )

            # Keep only newest 'keep_runs'
            allowed_runs = set(existing_complete_sorted[: self.keep_runs])

            logger.info(
                "Existing complete runs found: %s. Allowed runs for download: %s",
                existing_complete_sorted,
                allowed_runs,
            )
        else:
            allowed_runs = None  # meaning all runs allowed

        for run in runs:

            # NEW: Skip if download_latest_only and this run is not the newest
            if self.download_latest_only and newest_on_server:
                if run != newest_on_server:
                    logger.info(
                        "Skipping run %s — download_latest_only is enabled, newest is %s",
                        run,
                        newest_on_server,
                    )
                    continue

            # Skip if run is older than the newest allowed run
            if allowed_runs is not None and run not in allowed_runs:
                logger.info("Skipping run %s — a newer complete run exists", run)
                continue

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

                    # If run already complete, skip all steps
                    if (
                        allowed_runs is not None
                        and run in allowed_runs
                        and run in existing_complete
                    ):
                        logger.info("Run %s already complete — skipping all steps", run)
                        break

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

        if self.cleanup_enabled:
            self._cleanup_old_dates()
            self._cleanup_old_runs_for_date()

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

    def _cleanup_old_dates(self) -> None:
        """
        Remove old dated folders from storage, preserving only
        dates >= (self.date - keep_days + 1).
        """
        try:
            prefix = f"{self.dataset_name}/"
            entries = self.storage.list(prefix)

            # Extract top-level date folders: dataset_name/YYYYMMDD/*
            date_folders = set(
                p.split("/")[1]
                for p in entries
                if p.startswith(prefix) and len(p.split("/")) > 1
            )

            if not date_folders:
                return

            cutoff_date = self.date - timedelta(days=self.keep_days - 1)
            cutoff_str = cutoff_date.strftime("%Y%m%d")

            logger.info(
                "Cleanup enabled: keep_days=%s, cutoff >= %s",
                self.keep_days,
                cutoff_str,
            )

            for folder in date_folders:
                if not (len(folder) == 8 and folder.isdigit()):
                    continue

                if folder < cutoff_str:
                    folder_prefix = f"{self.dataset_name}/{folder}/"
                    logger.info("Deleting old folder: %s", folder_prefix)
                    self.storage.delete_prefix(folder_prefix)

                    # prune metadata
                    for var in self.metadata.keys():
                        self.metadata[var] = {
                            f: ts for f, ts in self.metadata[var].items() if folder in f
                        }

        except Exception as e:
            logger.error("Cleanup old folders failed: %s", e, exc_info=True)

    def _cleanup_old_runs_for_date(self):
        """Remove obsolete runs and forecast steps for the active date according to keep_runs."""

        date_str = self.date.strftime("%Y%m%d")
        base_prefix = f"{self.dataset_name}/{date_str}/"

        # Gather all objects under this date
        entries = self.storage.list(base_prefix)

        # Detect available runs
        runs_found = set()
        for key in entries:
            parts = key.split("/")
            # expected: dataset/date/run/var/file
            if len(parts) >= 4:
                run = parts[2]
                runs_found.add(run)

        if not runs_found:
            return

        # Count number of data files per run
        expected_total = len(self.dataset["variables"]) * len(
            self.dataset["forecast_steps"]
        )
        run_sizes = {run: 0 for run in runs_found}

        for key in entries:
            parts = key.split("/")
            if len(parts) >= 4:
                run = parts[2]
                if not key.endswith(".json"):
                    run_sizes[run] += 1

        # Select complete runs
        complete_runs = [
            run for run, count in run_sizes.items() if count == expected_total
        ]

        if not complete_runs:
            logger.info(
                "No complete runs found for date %s; skipping run cleanup.",
                date_str,
            )
            return

        # Sort runs newest → oldest
        sorted_runs = sorted(complete_runs, key=lambda r: int(r), reverse=True)

        # Keep the first N (default 1)
        keep = sorted_runs[: max(self.keep_runs, 1)]

        logger.info("For %s, keeping completed runs: %s", date_str, keep)

        # Delete all other complete runs
        for run in complete_runs:
            if run not in keep:
                prefix = f"{self.dataset_name}/{date_str}/{run}/"
                logger.info("Deleting obsolete run folder: %s", prefix)
                self.storage.delete_prefix(prefix)

        # Clean metadata
        keep_prefixes = [f"{date_str}{run}" for run in keep]

        for var in self.metadata.keys():
            new_entries = {}
            for fname, ts in self.metadata[var].items():
                if any(prefix in fname for prefix in keep_prefixes):
                    new_entries[fname] = ts
            self.metadata[var] = new_entries

    def _detect_existing_complete_runs(self) -> list[str]:
        """Return a list of completed runs already stored for this date."""

        date_str = self.date.strftime("%Y%m%d")
        prefix = f"{self.dataset_name}/{date_str}/"

        entries = self.storage.list(prefix)
        if not entries:
            return []

        expected_total = len(self.dataset["variables"]) * len(
            self.dataset["forecast_steps"]
        )

        runs_found = {key.split("/")[2] for key in entries if len(key.split("/")) >= 4}
        run_sizes = {r: 0 for r in runs_found}

        for key in entries:
            if key.endswith(".json"):
                continue
            parts = key.split("/")
            if len(parts) >= 4:
                run = parts[2]
                run_sizes[run] += 1

        complete = [run for run, count in run_sizes.items() if count == expected_total]

        return complete


def mirror_icon_dataset(
    dataset: Dict[str, Any],
    storage_cfg: Dict[str, Any],
    date: datetime,
) -> None:
    """
    Backwards-compatible wrapper to keep external call sites working.
    """
    IconDatasetMirror(dataset, storage_cfg, date).run()
