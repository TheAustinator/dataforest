from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.collectors import collect_hooks


class MetaDataProcess(MetaConfig):
    @property
    def _DEFAULT_ATTRS(cls):
        return cls._CONFIG["dataprocess_default_attrs"]

    @property
    def _HOOKS(cls):
        return {k: v for source in cls._CONFIG["hook_sources"] for k, v in collect_hooks(source).items()}

    @property
    def SETUP_HOOKS(cls):
        return [v for k, v in cls._HOOKS.items() if k in cls._CONFIG["setup_hooks"]]

    @property
    def CLEAN_HOOKS(cls):
        return [v for k, v in cls._HOOKS.items() if k in cls._CONFIG["clean_hooks"]]
