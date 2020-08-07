from pathlib import Path

from dataforest.utils.loaders.config import load_config


class MetaConfig(type):
    CONFIG = load_config(Path(__file__).parent.parent / "config/default_config.yaml")

    def __getitem__(self, item):
        return self.CONFIG[item]
