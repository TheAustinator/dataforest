from importlib import import_module
from operator import le, ge

from pathlib import Path

from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.plots_config import parse_plot_map


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
    def PLOT_MAP(cls):  # TODO-QC: process plot map starting here? Make it into a class where you can fetch plot_kwargs?
        plot_map = cls.CONFIG.get("plot_map", dict())
        plot_kwargs_defaults = cls.CONFIG.get("plot_kwargs_defaults", dict())
        return parse_plot_map(plot_map, plot_kwargs_defaults)

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
