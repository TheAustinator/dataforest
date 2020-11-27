from collections import Callable

from dataforest.core.DataBranch import DataBranch
from dataforest.utils import listify
from dataforest.utils.decorator_base import decorator_base

_DEFAULT_BIG_PLOT_RESOLUTION_PX = (1000, 1000)  # width, height in pixels
_PLOT_FILE_EXT = ".png"


# noinspection PyPep8Naming
class plot_base(decorator_base):
    def _wrap(self, func: Callable):
        def wrapper(branch: "DataBranch", *args, **kwargs):
            self._check_bypass()
            self._check_required_process(func, branch)
            return func(branch, *args, **kwargs)

        return wrapper

    def _check_required_process(self, func: Callable, branch: "DataBranch"):
        required = self._kwargs.pop("requires", None)
        if required:
            if not branch[required].success:
                raise ValueError(f"`{func.__name__}` requires a complete and successful process: `{required}`")
            precursors = branch.spec.get_precursors_lookup(incl_current=True)[branch.current_process]
            if required not in precursors:
                proc = required
                raise ValueError(
                    f"This plot method requires a branch at `{proc}` or later. Current process run: {precursors}. If "
                    f"`{proc}` has already been run, please use `branch.goto_process`. Otherwise, please run `{proc}`."
                )

    def _check_forbidden_kwargs(self, func: Callable, kwargs: dict):
        forbidden = self._kwargs.pop("forbid", None)
        if forbidden:
            forbidden = {forbidden} if isinstance(forbidden, str) else forbidden
            forbidden_found = set(forbidden).union(kwargs.keys())
            if forbidden_found:
                raise ValueError(f"Forbidden arguments: {forbidden_found} passed to {func.__name__}")

    def _check_bypass(self):
        if "bypass" in self._kwargs:
            self._bypass = listify(self._kwargs["bypass"])
        else:
            self._bypass = list()
