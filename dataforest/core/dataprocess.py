import gc
import logging
from functools import wraps
from typing import Callable, Optional, Union, List, Tuple

from pathlib import Path

from dataforest.utils.exceptions import InputDataNotFound


# noinspection PyPep8Naming
class dataprocess:
    """
    Decorator which allows allows for a process to work within the data_forest infrastructure.
    Each `data_process` is tied to two levels of subdirectory -- an outer directory which is named to match the process
    and an inner directory which is named to capture relevant details of the process run state
    ✓ uses `ForestQuery` to generate directory names corresponding to `DataForest.spec`
    ✓ checks for `partition` in spec and sets it in data for `comparative` analyses
    ✓ moves DataForest data selection to `process_name` using `at`
    ✓ checks to ensure that specified input data is present
    - may make new tables and update them in the future
    """

    def __init__(
            self,
            requires: str,
            comparative: bool = False,
            overwrite: bool = True,
            add_setup_hooks: Union[List, Tuple] = (),
            add_clean_hooks: Union[List, Tuple] = (),
            setup_hooks: Optional[Union[List, Tuple]] = None,
            clean_hooks: Optional[Union[List, Tuple]] = None,
    ):
        self.requires = requires
        self.comparative = comparative
        self.overwrite = overwrite
        self.forest = None
        self.setup_hooks = setup_hooks if setup_hooks is not None else self.SETUP_HOOKS
        self.clean_hooks = clean_hooks if clean_hooks is not None else self.CLEAN_HOOKS
        self.setup_hooks = list(self.setup_hooks) + list(add_setup_hooks)
        self.clean_hooks = list(self.clean_hooks) + list(add_clean_hooks)

    def __call__(self, func) -> Callable:
        self.process_name = func.__name__

        @wraps(func)
        def wrapper(forest, *args, **kwargs):
            self.forest = forest
            [getattr(self, hook.__name__)() for hook in self.setup_hooks]
            try:
                return func(self.forest, *args, **kwargs)
            except Exception as e:
                raise e
            finally:
                try:
                    [getattr(self, hook.__name__)() for hook in self.clean_hooks]
                except Exception as e:
                    e.message = "dataprocess - CLEAN_HOOK error: " + e.message
                    raise e

        return wrapper

    def _hook_get_process_forest(self):
        self.forest = self.forest.at(self.process_name)

    def _hook_comparative(self):
        """Sets up DataForest for comparative analysis"""
        if "partition" in self.forest.spec:
            logging.warning(
                "`partition` found at base level of spec. It should normally be specified under an individual process"
            )

        if self.comparative:
            partition = self.forest.spec[self.process_name].get("partition", None)
            if partition is None:
                example_dict = {self.process_name: {"partition": {"var_1", "var_2"}}}
                raise ValueError(
                    f"When `dataprocess` arg `comparative=True`, `forest.spec` must contain the key "
                    f"'partition' nested inside the decorated process name. I.e.: {example_dict}"
                )
            self.forest.set_partition(self.process_name)

    def _hook_input_exists(self):
        """Checks that input `ProcessRun` directory exists"""
        if not self.forest.paths[self.requires].exists():
            raise InputDataNotFound(self.forest, self.requires, self.process_name)
        contains_files = any(list(map(Path.is_file, self.forest.paths[self.requires].iterdir())))
        if not contains_files:
            raise InputDataNotFound(self.forest, self.requires, self.process_name)

    def _hook_mkdirs(self):
        """Makes directories for `ProcessRun` outputs"""
        path = self.forest.paths[self.process_name]
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    def _hook_overwrite(self):
        # TODO: fill in
        raise NotImplementedError()

    def _hook_garbage_collection(self):
        gc.collect()

    SETUP_HOOKS = (_hook_comparative, _hook_input_exists, _hook_mkdirs)
    CLEAN_HOOKS = (_hook_garbage_collection,)
