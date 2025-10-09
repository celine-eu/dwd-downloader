import yaml
from pathlib import Path


def load_dataset_config(config_path: Path):
    """Load dataset definitions from YAML config."""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("datasets", []), cfg.get("storage", {})
