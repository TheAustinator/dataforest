from functools import wraps
import traceback
from typing import Callable, Iterable, Optional, Union, List, Tuple

from dataforest.hooks.dataprocess.MetaDataProcess import MetaDataProcess
from dataforest.utils.exceptions import HookException


# noinspection PyPep8Naming
class dataprocess(metaclass=MetaDataProcess):
    """
    Decorator which allows allows for a processes to work within the data_forest infrastructure.
    Each `data_process` is tied to two levels of subdirectory -- an outer directory which is named to match the processes
    and an inner directory which is named to capture relevant details of the processes run state
    ✓ uses `ForestQuery` to generate directory names corresponding to `DataBranch.spec`
    ✓ checks for `partition` in branch_spec and sets it in data for `comparative` analyses
    ✓ moves DataBranch data selection to `process_name` using `at`
    ✓ checks to ensure that specified input data is present
    - may make new tables and update them in the future

    kwargs can be used to pass custom attributes
    """

    def __init__(
        self,
        add_setup_hooks: Union[List, Tuple] = (),
        add_clean_hooks: Union[List, Tuple] = (),
        setup_hooks: Optional[Union[List, Tuple]] = None,
        clean_hooks: Optional[Union[List, Tuple]] = None,
        **kwargs,
    ):
        self._name = None
        self.branch = None
        self.setup_hooks = setup_hooks if setup_hooks is not None else self.__class__.SETUP_HOOKS
        self.clean_hooks = clean_hooks if clean_hooks is not None else self.__class__.CLEAN_HOOKS
        self.setup_hooks = list(self.setup_hooks) + list(add_setup_hooks)
        self.clean_hooks = list(self.clean_hooks) + list(add_clean_hooks)
        # set custom attributes
        kwargs = {**self.__class__._DEFAULT_ATTRS, **kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def name(self):
        if self._name is None:
            raise ValueError("dataprocess name not set")
        return self._name

    def __call__(self, func) -> Callable:
        self.process = func.__name__

        @wraps(func)
        def wrapper(branch, run_name, stop_on_error=True, stop_on_hook_error=True, *args, **kwargs):
            """
            Runs setup hooks, then processes, then attempts each cleanup hook,
            raising any errors at the end.
            """
            self.branch = branch
            self._name = run_name
            self._run_hooks(self.setup_hooks, stop_on_hook_error=stop_on_hook_error)
            try:
                return func(self.branch, run_name, *args, **kwargs)
            except Exception as e:
                self._handle_error(e, "process.err", stop_on_error)
            finally:
                self._run_hooks(self.clean_hooks, try_all=True, stop_on_hook_error=stop_on_hook_error)

        self.func = wrapper
        return wrapper

    def _run_hooks(self, hooks: Iterable[Callable], try_all: bool = False, stop_on_hook_error: bool = True):
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
                    self._handle_error(HookException(self.name, hook_exceptions), "hooks.err", stop_on_hook_error)
        if hook_exceptions:
            self._handle_error(HookException(self.name, hook_exceptions), "hooks.err", stop_on_hook_error)

    def _handle_error(self, e, logfile_name, stop):
        try:
            self._log_error(e, logfile_name)
        finally:
            if stop:
                raise e

    def _log_error(self, e, logfile_name):
        branch = self.branch
        log_dir = branch[branch.current_process].logs_path
        log_dir.mkdir(exist_ok=True)
        log_path = log_dir / logfile_name
        with open(log_path, "w") as f:
            f.write("Traceback (most recent call last):")
            traceback.print_tb(e.__traceback__, file=f)
            f.write(f"\n{str(e)}")
