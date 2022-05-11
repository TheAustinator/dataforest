import warnings

from pandas.errors import DtypeWarning

from dataforest.api import norm, metric, pair, jp, pl
from dataforest.utils.loaders.config import CONFIG_OPTIONS, load_config as _load_config
from dataforest.utils.loaders.update_config import get_current_config, get_config_updater as _get_config_updater
from dataforest.core.Interface import Interface

update_config = _get_config_updater(_load_config)

load = Interface.load
from_input_dirs = Interface.from_input_dirs
from_sample_metadata = Interface.from_sample_metadata

warnings.filterwarnings("ignore", category=DtypeWarning)
