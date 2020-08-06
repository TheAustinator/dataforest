from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.collectors import collect_plots


class MetaPlotMethods(MetaConfig):
    @property
    def PLOT_METHOD_LOOKUP(cls):
        return {k: v for source in cls.CONFIG["plot_sources"] for k, v in collect_plots(source).items()}

    @property
    def PLOT_METHODS(cls):
        return cls.CONFIG["plot_methods"]
