from pathlib import Path
from typing import Tuple, Dict, Any
import yaml
import os


def load_dataset_config(path: Path) -> Tuple[list[Dict[str, Any]], Dict[str, Any]]:
    """
    Load dataset config from YAML and expand environment variables.
    """

    raw_text = path.read_text(encoding="utf-8")
    expanded_text = os.path.expandvars(raw_text)
    cfg = yaml.safe_load(expanded_text)

    return cfg.get("datasets", []), cfg.get("storage", {})
