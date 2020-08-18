from functools import wraps

from dataforest.config.MetaPlotMethods import MetaPlotMethods
from dataforest.utils import tether, copy_func
from dataforest.utils.ExceptionHandler import ExceptionHandler
from dataforest.utils.plots_config import get_plot_name_from_plot_method


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

    @property
    def plot_method_lookup(self):
        return self.__class__.PLOT_METHOD_LOOKUP

    @property
    def plot_methods(self):
        return self.__class__.PLOT_METHODS

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
                plot_name = get_plot_name_from_plot_method(
                    branch.plot.plot_methods[branch.current_process], method_name
                )

                if plot_name in process_run.plot_map:
                    for plot_kwargs_key, plot_filename in process_run.plot_map[plot_name].items():
                        plot_dir = plot_filename.parent  # only need one sample dir

                plot_dir.mkdir(exist_ok=True)
                return method(branch, *args, **kwargs)
            except Exception as e:
                err_filename = method.__name__
                ExceptionHandler.handle(self.branch, e, err_filename, stop_on_error)

        return wrapped
