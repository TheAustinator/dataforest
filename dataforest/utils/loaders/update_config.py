from pathlib import Path
from typing import Union, Callable

from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.config import load_config


def get_config_updater(config_loader: Callable) -> Callable:
    def _update_config(config: Union[dict, str, Path]):
        config = config_loader(config)
        MetaConfig.CONFIG = config

    return _update_config


def get_current_config() -> dict:
    return MetaConfig.CONFIG
