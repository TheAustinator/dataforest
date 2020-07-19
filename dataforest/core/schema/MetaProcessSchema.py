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
    def STANDARD_FILES(cls):
        return cls._CONFIG["standard_files"]

    @property
    def FILE_MAP(cls):
        return cls._CONFIG["file_map"]

    @property
    def R_FILENAMES(cls):
        return cls._CONFIG.get("r_filenames", {})

    @property
    def R_FILEPATHS(cls):
        return cls._get_r_filepaths(cls.R_SCRIPTS_DIR, cls.R_FILENAMES)

    @property
    def R_SCRIPTS_DIR(cls):
        path = cls._CONFIG.get("r_scripts_module")
        if path:
            path = import_module(path).__path__[0]
        return path

    @property
    def TEMP_METADATA_FILENAME(cls):
        return cls._CONFIG.get("temp_meta_filename")

    @staticmethod
    def _get_r_filepaths(scripts_dir, r_filenames):
        # has to be a function due to scoping issue with class var dict comprehension
        return {k: Path(scripts_dir) / v for k, v in r_filenames.items()}
