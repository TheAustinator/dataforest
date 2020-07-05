import json
from pathlib import Path
from typing import Union

import yaml


def load_config(config: Union[dict, str, Path]):
    """Load library loaders"""
    if isinstance(config, (str, Path)):
        config = Path(config)
        if config.suffix == ".json":
            with open(config, "r") as f:
                config = json.load(f)
        elif config.suffix == ".yaml":
            with open(config, "r") as f:
                config = yaml.load(f, yaml.FullLoader)
        else:
            raise ValueError("If filepath is passed, must be .json or .yaml")
    if not isinstance(config, dict):
        raise TypeError(
            f"Config must be either a `dict` or a path to a .json or .yaml file as either a `str` or `pathlib.Path`"
        )
    return config
