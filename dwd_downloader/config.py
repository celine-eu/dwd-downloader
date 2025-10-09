import yaml
from pathlib import Path

def load_config(path: str = "config.yaml"):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    cfg["data_dir"] = str(Path(cfg["data_dir"]).resolve())
    return cfg
