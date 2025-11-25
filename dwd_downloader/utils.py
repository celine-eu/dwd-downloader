# utils.py
from __future__ import annotations

import bz2
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Iterable

import requests

from .logger import get_logger
from .storage import Storage

logger = get_logger(__name__)


def _metadata_dict(
    url: str,
    sha256: str,
    size_bytes: int,
    http_headers: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    metadata: Dict[str, object] = {
        "url": url,
        "sha256": sha256,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "size_bytes": size_bytes,
    }
    if http_headers:
        if "Last-Modified" in http_headers:
            metadata["http_last_modified"] = http_headers.get("Last-Modified")
        if "ETag" in http_headers:
            metadata["http_etag"] = http_headers.get("ETag")
        metadata["http_headers"] = {
            k: v
            for k, v in http_headers.items()
            if k in ("Content-Type", "Content-Length")
        }
    return metadata


def _stream_decompressed_bz2(
    response: requests.Response,
) -> Iterable[bytes]:
    decompressor = bz2.BZ2Decompressor()
    for chunk in response.iter_content(1024 * 1024):
        if not chunk:
            continue
        data = decompressor.decompress(chunk)
        if data:
            yield data
    # BZ2Decompressor has no flush; any remaining data is returned during decompress calls.


def _stream_raw(response: requests.Response) -> Iterable[bytes]:
    for chunk in response.iter_content(1024 * 1024):
        if chunk:
            yield chunk


def download_to_storage(
    url: str,
    storage: Storage,
    data_key: str,
    meta_key: Optional[str] = None,
    decompress: bool = False,
    timeout: int = 30,
) -> bool:
    """
    Stream a remote file directly into the storage backend.

    - If decompress=True and the source is .bz2, we decompress in-memory and store
      the decompressed bytes at `data_key`.
    - No intermediate files on local disk are created (only streaming).
    - A small JSON metadata sidecar is written to `meta_key` if provided.

    Returns:
        True if download completed successfully, False if remote was not available.
    """
    try:
        with requests.get(url, stream=True, timeout=timeout) as resp:
            if resp.status_code != 200:
                logger.warning("Failed to download %s (code %s)", url, resp.status_code)
                return False

            hasher = hashlib.sha256()
            size_bytes = 0

            # Build a generator that both computes metadata and yields bytes
            if decompress:
                raw_stream = _stream_decompressed_bz2(resp)
            else:
                raw_stream = _stream_raw(resp)

            def meta_stream():
                nonlocal size_bytes
                for chunk in raw_stream:
                    hasher.update(chunk)
                    size_bytes += len(chunk)
                    yield chunk

            # First: write file bytes
            storage.write_stream(data_key, meta_stream())

            # Then: write metadata JSON if requested
            if meta_key:
                metadata = _metadata_dict(
                    url,
                    sha256=hasher.hexdigest(),
                    size_bytes=size_bytes,
                    http_headers=dict(resp.headers),
                )
                payload = json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8")
                storage.write_stream(meta_key, (payload,))
            return True

    except requests.HTTPError as he:
        logger.warning("HTTP error downloading %s: %s", url, he)
        return False
    except Exception as e:
        logger.error("Error downloading %s: %s", url, e, exc_info=True)
        return False
