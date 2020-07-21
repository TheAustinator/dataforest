from importlib import import_module
from operator import le, ge

from pathlib import Path

from dataforest.config.MetaConfig import MetaConfig


class MetaProcessSchema(MetaConfig):
    @property
    def SUBSET_PROXIES(cls):
        return {
            "max_lane_id": (le, "lane_id"),
            "min_lane_id": (ge, "lane_id"),
        }

    @property
    def FILE_MAP(cls):
        return cls["file_map"]

    @property
    def LAYERS(cls):
        return cls["layers"]

    @property
    def PROCESS_LAYERS(cls):
        return cls["process_layers"]

    @property
    def TEMP_METADATA_FILENAME(cls):
        return cls["temp_meta_filename"]

    @staticmethod
    def _get_r_filepaths(scripts_dir, r_filenames):
        # has to be a function due to scoping issue with class var dict comprehension
        return {k: Path(scripts_dir) / v for k, v in r_filenames.items()}
