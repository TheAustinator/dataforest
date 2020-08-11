from functools import wraps

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

    @property
    def plot_method_lookup(self):
        return self.__class__.PLOT_METHOD_LOOKUP

    @property
    def plot_methods(self):
        return self.__class__.PLOT_METHODS

    def _wrap(self, method):
        """Wrap with mkdirs and logging"""

        @wraps(method)
        def wrapped(branch, method_name, *args, stop_on_error: bool = False, **kwargs):
            try:
                process_run = branch[branch.current_process]
                plot_name = method_name.replace("plot_", "", 1)

                if plot_name in process_run.plot_map:
                    plot_dir = process_run.plot_map[plot_name].parent
                elif method_name in process_run.plot_map:
                    plot_dir = process_run.plot_map[method_name].parent

                plot_dir.mkdir(exist_ok=True)
                return method(branch, *args, **kwargs)
            except Exception as e:
                err_filename = method.__name__
                ExceptionHandler.handle(self.branch, e, err_filename, stop_on_error)

        return wrapped
