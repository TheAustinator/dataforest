from pathlib import Path
from typing import List

from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.collectors import collect_plots
from dataforest.utils.loaders.path import get_module_paths
from dataforest.utils.plots_config import build_process_plot_method_lookup, parse_plot_kwargs


class MetaPlotMethods(MetaConfig):
    @property
    def PLOT_METHOD_LOOKUP(cls):
        return {k: v for source in cls.CONFIG["plot_sources"] for k, v in collect_plots(source).items()}

    @property
    def PLOT_MAP(cls):
        return cls.CONFIG.get("plot_map", dict())

    @property
    def PROCESS_PLOT_METHODS(cls):
        try:
            plot_methods = cls.CONFIG["plot_methods"]
        except KeyError:
            plot_methods = build_process_plot_method_lookup(cls.CONFIG.get("plot_map", dict()))

        return plot_methods

    @property
    def PLOT_KWARGS_DEFAULTS(cls):
        return cls.CONFIG.get("plot_kwargs_defaults", dict())

    @property
    def PLOT_KWARGS(cls):  # TODO-QC: mapping of process, plot to plot_kwargs
        plot_map = cls.CONFIG.get("plot_map", dict())
        plot_kwargs_defaults = cls.CONFIG.get("plot_kwargs_defaults", dict())
        plot_kwargs = parse_plot_kwargs(plot_map, plot_kwargs_defaults)
        return plot_kwargs

    # @property
    # def R_FUNCTIONS_FILEPATH(cls) -> Path:
    #     return get_module_paths([cls.CONFIG["r_functions_sources"]])[0]

    @property
    def R_PLOT_SOURCES(cls) -> List[Path]:
        return get_module_paths(cls.CONFIG["r_plot_sources"])
