# dwd_downloader/utils.py
from __future__ import annotations

import bz2
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict
import requests
from .logger import get_logger

logger = get_logger(__name__)


def sha256sum(path: Path) -> str:
    """Compute SHA256 checksum of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_metadata(
    json_path: Path,
    url: str,
    file_path: Path,
    http_headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Write a JSON sidecar with metadata for `file_path`.
    json_path: final metadata path (e.g. file.grib2.json or file.grib2.bz2.json)
    """
    metadata = {
        "url": url,
        "sha256": sha256sum(file_path),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "size_bytes": file_path.stat().st_size,
    }

    if http_headers:
        # include common HTTP headers if available
        if "Last-Modified" in http_headers:
            metadata["http_last_modified"] = http_headers.get("Last-Modified")
        if "ETag" in http_headers:
            metadata["http_etag"] = http_headers.get("ETag")
        metadata["http_headers"] = {
            k: v
            for k, v in http_headers.items()
            if k in ("Content-Type", "Content-Length")
        }

    # ensure parent exists
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(metadata, jf, indent=2, sort_keys=True)
        logger.debug(f"Wrote metadata file {json_path}")


def download_file(
    url: str,
    dest: Path,
    decompress: bool = False,
    overwrite: bool = False,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> bool:
    """
    Atomically download a URL to dest. Create a JSON sidecar with checksum.
    - Downloads into a temporary .part file in the destination directory and renames atomically.
    - Skips the download if dest exists and overwrite is False.
    - If decompress is True and dest ends with '.bz2', the final file will be the decompressed filename
      (i.e. 'file.grib2' from 'file.grib2.bz2'), and the JSON will be written next to that final file.
    Returns True if a file was downloaded and written; False if skipped or failed.
    """
    # Quick skip if already present
    if dest.exists() and not overwrite:
        # If the data file already exists, skip download.
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    sess = session or requests.Session()

    # Create temp file in same directory to ensure atomic rename works across filesystems
    tmp_file = None
    response = None
    try:
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=dest.parent, suffix=".part")
        tmp_file = Path(tmp.name)
        # perform GET streaming
        with sess.get(url, stream=True, timeout=timeout) as resp:
            response = resp
            if resp.status_code != 200:
                # remote not available yet
                tmp.close()
                tmp_file.unlink(missing_ok=True)
                logger.warning(f"Failed to download {url} (code {resp.status_code})")
                return False

            for chunk in resp.iter_content(1024 * 1024):
                if chunk:
                    tmp.write(chunk)
            tmp.flush()
            tmp.close()

        # determine final write behavior
        if decompress and dest.suffix == ".bz2":
            # decompress to final_path (remove .bz2 suffix)
            final_path = dest.with_suffix("")  # e.g. 'file.grib2' from 'file.grib2.bz2'
            try:
                with open(tmp_file, "rb") as f:
                    decompressed = bz2.decompress(f.read())
                with open(final_path, "wb") as out:
                    out.write(decompressed)
                tmp_file.unlink(missing_ok=True)
                # metadata sidecar: same filename plus ".json"
                meta_path = final_path.with_suffix(final_path.suffix + ".json")
                write_metadata(
                    meta_path,
                    url,
                    final_path,
                    http_headers=(
                        dict(response.headers) if response is not None else None
                    ),
                )
                return True
            except Exception:
                tmp_file.unlink(missing_ok=True)
                return False
        else:
            # move temp to final destination
            try:
                tmp_file.replace(dest)
            except Exception:
                tmp_file.unlink(missing_ok=True)
                return False
            # metadata sidecar next to the stored file (add .json to suffix)
            meta_path = dest.with_suffix(dest.suffix + ".json")
            write_metadata(
                meta_path,
                url,
                dest,
                http_headers=dict(response.headers) if response is not None else None,
            )
            return True

    except Exception:
        if tmp_file is not None and tmp_file.exists():
            tmp_file.unlink(missing_ok=True)
        return False
