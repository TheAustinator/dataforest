from functools import wraps
from typing import Callable, Iterable, Optional, Union, List, Tuple

from dataforest.hooks.dataprocess.MetaDataProcess import MetaDataProcess
from dataforest.utils.exceptions import HookException


# noinspection PyPep8Naming
class dataprocess(metaclass=MetaDataProcess):
    """
    Decorator which allows allows for a processes to work within the data_forest infrastructure.
    Each `data_process` is tied to two levels of subdirectory -- an outer directory which is named to match the processes
    and an inner directory which is named to capture relevant details of the processes run state
    ✓ uses `ForestQuery` to generate directory names corresponding to `DataForest.spec`
    ✓ checks for `partition` in spec and sets it in data for `comparative` analyses
    ✓ moves DataForest data selection to `process_name` using `at`
    ✓ checks to ensure that specified input data is present
    - may make new tables and update them in the future

    kwargs can be used to pass custom attributes
    """

    def __init__(
        self,
        requires: Optional[str] = None,
        comparative: bool = False,
        overwrite: bool = True,
        add_setup_hooks: Union[List, Tuple] = (),
        add_clean_hooks: Union[List, Tuple] = (),
        setup_hooks: Optional[Union[List, Tuple]] = None,
        clean_hooks: Optional[Union[List, Tuple]] = None,
        **kwargs,
    ):
        self.requires = requires
        self.comparative = comparative
        self.overwrite = overwrite
        self.forest = None
        self.setup_hooks = setup_hooks if setup_hooks is not None else self.__class__.SETUP_HOOKS
        self.clean_hooks = clean_hooks if clean_hooks is not None else self.__class__.CLEAN_HOOKS
        self.setup_hooks = list(self.setup_hooks) + list(add_setup_hooks)
        self.clean_hooks = list(self.clean_hooks) + list(add_clean_hooks)
        # set custom attributes
        kwargs = {**self.__class__._DEFAULT_ATTRS, **kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __call__(self, func) -> Callable:
        self.process_name = func.__name__

        @wraps(func)
        def wrapper(forest, *args, **kwargs):
            """
            Runs setup hooks, then processes, then attempts each cleanup hook,
            raising any errors at the end.
            """
            self.forest = forest
            self._run_hooks(self.setup_hooks)
            try:
                return func(self.forest, *args, **kwargs)
            except Exception as e:
                raise e
            finally:
                self._run_hooks(self.clean_hooks, try_all=True)

        return wrapper

    def _run_hooks(self, hooks: Iterable[Callable], try_all: bool = False):
        """
        Args:
            hooks: hooks to run
            try_all: whether or not to try all and raise exceptions at the end
                rather than halting excecution at first exception
        """
        hook_exceptions = {}
        for hook in hooks:
            try:
                hook(self)
            except Exception as e:
                hook_exceptions[str(hook.__name__)] = e
                if not try_all:
                    raise HookException(self.process_name, hook_exceptions)
        if hook_exceptions:
            raise HookException(self.process_name, hook_exceptions)
