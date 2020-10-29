import json
from pathlib import Path
from typing import Union, AnyStr, Dict, Callable

import yaml

from dataforest import config as _config_module


def get_config_options(config_dir: AnyStr) -> Dict[str, Path]:
    config_paths = Path(config_dir).iterdir()
    config_lookup = {path.stem: path for path in config_paths if not path.name.startswith("__")}
    return config_lookup


def get_config_loader(config_options: Dict[str, Path]) -> Callable:
    def _load_config(config: Union[dict, str, Path]) -> dict:
        """
        Global configuration for package function from dict, filepath, or name
        in `config_options`
        """
        if isinstance(config, (str, Path)):
            if config in config_options:
                config = config_options[config]
            config = Path(config)
            if config.suffix == ".json":
                with open(str(config), "r") as f:
                    config = json.load(f)
            elif config.suffix == ".yaml":
                with open(str(config), "r") as f:
                    config = yaml.load(f, yaml.FullLoader)
            else:
                raise ValueError("If filepath is passed, must be .json or .yaml")
        if not isinstance(config, dict):
            raise TypeError(
                f"Config must be either a `dict` or a path to a .json or .yaml file as either a `str` or `pathlib.Path`"
            )
        return config

    return _load_config


_CONFIG_DIR = Path(_config_module.__file__).parent
CONFIG_OPTIONS = get_config_options(_CONFIG_DIR)
load_config = get_config_loader(CONFIG_OPTIONS)
