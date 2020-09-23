from functools import wraps
from pathlib import Path
from typing import Optional, Dict

from dataforest.config.MetaPlotMethods import MetaPlotMethods
from dataforest.utils import tether, copy_func
from dataforest.utils.ExceptionHandler import ExceptionHandler


class PlotMethods(metaclass=MetaPlotMethods):
    """
    Container class for static plotting methods, which should accept a
    DataBranch or subclass.
    # TODO: extend for DataTree or make MetaPlotMethods
    """

    def __init__(self, branch: "DataBranch"):
        self.branch = branch
        for name, plot_method in self.plot_method_lookup.items():
            callable_ = copy_func(plot_method)
            callable_.__name__ = name
            setattr(self, name, self._wrap(callable_))
        tether(self, "branch")

    def regenerate_plots(self, plot_map: Optional[Dict[str, str]]):
        raise NotImplementedError()

    @property
    def plot_method_lookup(self):
        return self.__class__.PLOT_METHOD_LOOKUP

    @property
    def plot_methods(self):
        return self.__class__.PROCESS_PLOT_METHODS

    @property
    def global_plot_methods(self):
        global_plot_methods = {
            config_name: callable_name
            for name_mapping in self.plot_methods.values()
            for config_name, callable_name in name_mapping.items()
        }
        return global_plot_methods

    @property
    def global_plot_methods_reverse(self):
        return {v: k for k, v in self.global_plot_methods.items()}

    @property
    def plot_kwargs_defaults(self):
        return self.__class__.PLOT_KWARGS_DEFAULTS

    @property
    def plot_kwargs(self):
        return self.__class__.PLOT_KWARGS

    def _wrap(self, method):
        """Wrap with mkdirs and logging"""

        @wraps(method)
        def wrapped(branch, method_name, *args, stop_on_error: bool = False, **kwargs):
            try:
                process_run = branch[branch.current_process]
                plot_name = branch.plot.global_plot_methods_reverse.get(method_name, None)
                if plot_name in process_run.plot_map:
                    if not (plt_filename_lookup := process_run.plot_map[plot_name]):
                        if plt_filename_lookup:
                            _, plt_filepath = next(iter(plt_filename_lookup.items()))
                            plt_dir = plt_filepath.parent
                        else:
                            plt_dir = Path("/tmp")
                        plt_dir.mkdir(exist_ok=True)
                return method(branch, *args, **kwargs)
            except Exception as e:
                err_filename = method.__name__
                ExceptionHandler.handle(self.branch, e, err_filename, stop_on_error)

        return wrapped
