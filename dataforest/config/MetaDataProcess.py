from dataforest.config.MetaConfig import MetaConfig
from dataforest.utils.loaders.collectors import collect_hooks


class MetaDataProcess(MetaConfig):
    @property
    def _DEFAULT_ATTRS(cls):
        return cls.CONFIG["dataprocess_default_attrs"]

    @property
    def _HOOKS(cls):
        return {k: v for source in cls.CONFIG["hook_sources"] for k, v in collect_hooks(source).items()}

    @property
    def SETUP_HOOKS(cls):
        return [cls._HOOKS[hook_name] for hook_name in cls.CONFIG["setup_hooks"] if hook_name in cls._HOOKS]

    @property
    def CLEAN_HOOKS(cls):
        return [cls._HOOKS[hook_name] for hook_name in cls.CONFIG["clean_hooks"] if hook_name in cls._HOOKS]
