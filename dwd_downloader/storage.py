# storage.py
import os
import io
import boto3
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterable, BinaryIO
from .logger import get_logger
from typing import BinaryIO, cast

logger = get_logger(__name__)


def get_storage(storage_cfg: Dict[str, Any]) -> "Storage":
    """Initialize storage backend from config or env."""
    stype = os.getenv("STORAGE_TYPE", None) or storage_cfg.get("type", "fs")

    if stype == "fs":
        data_dir = os.getenv("DWD_DATA_DIR", None)
        if data_dir:
            logger.info("Using %s (DWD_DATA_DIR) for data storage", data_dir)
        else:
            data_dir = storage_cfg.get("data_dir", None)
            if data_dir:
                logger.info("Using %s (config) for data storage", data_dir)
            else:
                data_dir = "./data"
                logger.info("Using %s (default) for data storage", data_dir)

        return FSStorage(base_dir=data_dir)

    elif stype == "s3":
        bucket = (
            os.getenv("AWS_BUCKET") or os.getenv("S3_BUCKET") or storage_cfg["bucket"]
        )
        endpoint = (
            os.getenv("AWS_ENDPOINT_URL")
            or os.getenv("S3_ENDPOINT_URL")
            or storage_cfg.get("endpoint_url")
        )
        return S3Storage(bucket=bucket, endpoint_url=endpoint)

    else:
        raise ValueError(f"Unknown storage type: {stype}")


class Storage(ABC):
    """
    Abstract base for storage backends.

    All read/write must go through this interface (FS or S3).
    """

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str = "") -> List[str]: ...

    @abstractmethod
    def write_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        """
        Write an iterable of bytes `chunks` to the given key.
        Must not assume data fits in memory.
        """
        ...

    @abstractmethod
    def open(self, key: str, mode: str = "rb") -> BinaryIO:
        """
        Open a key for reading (mode='rb').
        Write mode is not required – `write_stream` should be used instead.
        """
        ...


class FSStorage(Storage):
    """Filesystem-based storage with streaming support."""

    def __init__(self, base_dir: str = "./data"):
        self.base_dir = os.path.abspath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)

    def _full_path(self, key: str) -> str:
        return os.path.join(self.base_dir, key)

    def exists(self, key: str) -> bool:
        return os.path.exists(self._full_path(key))

    def list(self, prefix: str = "") -> List[str]:
        results: List[str] = []
        for root, _, files in os.walk(self.base_dir):
            for f in files:
                rel = os.path.relpath(os.path.join(root, f), self.base_dir)
                if rel.startswith(prefix):
                    results.append(rel)
        return results

    def write_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        path = self._full_path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            for chunk in chunks:
                if chunk:
                    f.write(chunk)

    def open(self, key: str, mode: str = "rb") -> BinaryIO:
        if "r" in mode and "+" not in mode and "w" not in mode:
            path = self._full_path(key)
            f = open(path, mode)
            return cast(BinaryIO, f)
        raise ValueError("FSStorage.open only supports read mode ('rb').")


class _IterableReader(io.RawIOBase):
    """
    Wraps an iterable of bytes into a file-like object (for boto3.upload_fileobj).
    """

    def __init__(self, chunks: Iterable[bytes]):
        super().__init__()
        self._iter = iter(chunks)
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        if not self._buffer:
            try:
                self._buffer = next(self._iter)
            except StopIteration:
                return 0  # EOF
        n = min(len(b), len(self._buffer))
        b[:n] = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return n


class S3Storage(Storage):
    """S3/Minio-based storage with streaming support."""

    def __init__(self, bucket: str, endpoint_url: str | None = None):
        self.bucket = bucket
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            or os.getenv("S3_SECRET_ACCESS_KEY"),
        )

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3.exceptions.ClientError:
            return False

    def list(self, prefix: str = "") -> List[str]:
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def write_stream(self, key: str, chunks: Iterable[bytes]) -> None:
        reader = _IterableReader(chunks)
        self.s3.upload_fileobj(reader, self.bucket, key)

    def open(self, key: str, mode: str = "rb") -> BinaryIO:
        if "r" in mode and "+" not in mode and "w" not in mode:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            # StreamingBody is file-like
            return obj["Body"]
        raise ValueError("S3Storage.open only supports read mode ('rb').")
