from functools import wraps
import logging
from pathlib import Path
from typing import Optional, Dict, Iterable, TYPE_CHECKING, Set

from IPython.display import Image, display
from typeguard import typechecked

from dataforest.config.MetaPlotMethods import MetaPlotMethods
from dataforest.utils import tether, copy_func
from dataforest.utils.ExceptionHandler import ExceptionHandler

if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch


class PlotMethods(metaclass=MetaPlotMethods):
    """
    Container class for static plotting methods, which should accept a
    DataBranch or subclass.
    # TODO: extend for DataTree or make MetaPlotMethods
    """

    def __init__(self, branch: "DataBranch"):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.branch = branch
        for name, plot_method in self.plot_method_lookup.items():
            callable_ = copy_func(plot_method)
            callable_.__name__ = name
            setattr(self, name, self._wrap(callable_))
        tether(self, "branch", incl_methods=list(self.plot_method_lookup.keys()))
        # self._plot_cache = {process: PlotCache(self.branch, process) for process in self.branch.spec.processes}
        self._img_cache = {}

    def display(self, process_name: str):
        plots_path = self.branch[process_name].plots_path
        for img_path in plots_path.iterdir():
            display(Image(img_path))

    @typechecked
    def generate_plots(
        self,
        processes: Optional[Iterable[str]] = None,
        plot_map: Optional[Dict[str, dict]] = None,
        plot_kwargs: Optional[Dict[str, dict]] = None,
    ):
        plot_map = self.plot_map if not plot_map else plot_map
        plot_kwargs = self.plot_kwargs if not plot_kwargs else plot_kwargs
        if processes is not None:
            plot_map = {proc: proc_plot_map for proc, proc_plot_map in plot_map.items() if proc in processes}
            plot_kwargs = {
                proc: proc_plot_kwargs for proc, proc_plot_kwargs in plot_kwargs.items() if proc in processes
            }
        for process, proc_plot_map in plot_map.items():
            method_names_config = tuple(proc_plot_map.keys())
            for name_config in method_names_config:
                method_name = self.method_name_lookup[name_config]
                method = getattr(self, method_name)
                kwarg_sets = plot_kwargs[process][name_config].values()
                for kwargs in kwarg_sets:
                    method(**kwargs)

    @property
    def plot_method_lookup(self):
        return self.__class__.PLOT_METHOD_LOOKUP

    @property
    def plot_methods(self):
        return self.__class__.PROCESS_PLOT_METHODS

    @property
    def method_lookup(self):
        return {k: getattr(self, method_name) for k, method_name in self.method_name_lookup.items()}

    @property
    def method_name_lookup(self):
        global_plot_methods = {
            config_name: callable_name
            for name_mapping in self.plot_methods.values()
            for config_name, callable_name in name_mapping.items()
        }
        return global_plot_methods

    @property
    def method_key_lookup(self):
        return {v: k for k, v in self.method_name_lookup.items()}

    @property
    def plot_kwargs_defaults(self):
        return self.__class__.PLOT_KWARGS_DEFAULTS

    @property
    def plot_map(self):
        # TODO: rename to plot_settings for clarity and to avoid confusion w/ process run?
        return self.__class__.PLOT_MAP

    @property
    def plot_kwargs(self):
        return self.__class__.PLOT_KWARGS

    @property
    def methods(self) -> Dict[str, Set[str]]:
        """
        Key: process_name at which plot becomes unlocked
        Value: plot method names
        """
        is_plot_method = lambda s: s.startswith("plot") and callable(getattr(self, s))
        method_names = list(filter(is_plot_method, dir(self)))
        avail_dict = {}

        def _assign(method_name):
            method = getattr(self, method_name)
            if hasattr(method, "_requires"):
                required = getattr(method, "_requires")
            else:
                required = "root"
            if required not in avail_dict:
                avail_dict[required] = set()
            avail_dict[required].add(method_name)

        list(map(_assign, method_names))
        return avail_dict

    @property
    def keys(self) -> Dict[str, Set[str]]:
        """
        Key: process name at which plot becomes unlocked
        Value: keys for plots in config
        """
        convert_to_key = lambda set_: set(map(lambda s: "_" + s.upper()[5:] + "_", set_))
        return {k: convert_to_key(v) for k, v in self.methods.items()}

    def _wrap(self, method):
        """Wrap with mkdirs and logging"""

        @wraps(method)
        def wrapped(branch, method_name, *args, stop_on_error: bool = False, **kwargs):
            try:
                process_run = branch[branch.current_process]
                plot_name = branch.plot.method_key_lookup.get(method_name, None)
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
                ExceptionHandler.handle(self.branch, e, f"PLOT_{err_filename}.err", stop_on_error)

        return wrapped

    # def __getitem__(self, key):
