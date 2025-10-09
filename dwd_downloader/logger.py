import os
import logging
from pathlib import Path
from typing import Optional


def get_logger(
    name: str = "dwd_downloader",
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    console: bool = True,
) -> logging.Logger:
    """
    Centralized logger for the library.
    Args:
        name: logger name
        level: logging level (default INFO)
        log_file: optional file path to write logs
        console: enable stdout logging
    Returns:
        logger instance
    """
    logger = logging.getLogger(name)

    env_level = os.getenv("LOG_LEVEL")
    if env_level:
        env_level_val = logging.getLevelNamesMapping().get(env_level.upper(), None)
        if env_level_val:
            level = env_level_val
        else:
            logger.warning(
                f"LOG_LEVEL={env_level} is not valid. Valid values are {", ".join(logging.getLevelNamesMapping().keys())}"
            )

    logger.setLevel(level)

    if logger.hasHandlers():
        # already configured
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
