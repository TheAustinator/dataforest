from pathlib import Path

from dataforest.utils.loaders.load_config import load_config


class MetaConfig(type):
    _CONFIG = load_config(Path(__file__).parent.parent / "config/default_config.yaml")
