from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.collectors import collect_processes


class MetaDataProcess(MetaConfig):
    @property
    def PROCESSES(cls):
        return {k: v for source in cls.CONFIG["process_sources"] for k, v in collect_processes(source).items()}
