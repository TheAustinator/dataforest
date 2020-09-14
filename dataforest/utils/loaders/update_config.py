from pathlib import Path
from typing import Union

from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.config import load_config


def update_config(config: Union[dict, str, Path]):
    config = load_config(config)
    MetaConfig.CONFIG = config


def get_current_config() -> dict:
    return MetaConfig.CONFIG
