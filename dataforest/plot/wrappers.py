from functools import wraps
import json
import logging
from pathlib import Path
from itertools import product
from typing import Tuple, Optional, AnyStr, Union

from IPython.display import Image, display
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from dataforest.core.DataBranch import DataBranch
from dataforest.core.PlotMethods import PlotMethods
from dataforest.plot.PlotPreparator import PlotPreparator

_DEFAULT_BIG_PLOT_RESOLUTION_PX = (1000, 1000)  # width, height in pixels
_PLOT_FILE_EXT = ".png"


def plot_py(plot_func):
    @wraps(plot_func)
    def wrapper(
        branch: "DataBranch",
        stratify: Optional[str] = None,
        facet: Optional[str] = None,
        plot_path: Optional[AnyStr] = None,
        facet_dim: tuple = (),
        show: bool = True,
        **kwargs,
    ) -> Union[plt.Figure, Tuple[plt.Figure, np.ndarray]]:
        prep = PlotPreparator(branch)
        stratify = None if stratify in prep.NONE_VARIANTS else stratify
        facet = None if facet in prep.NONE_VARIANTS else facet
        if facet is not None:
            kwargs["ax"] = prep.facet(facet, *facet_dim)
        if stratify is not None:
            prep.stratify(stratify, kwargs)
        if plot_path is not None:
            matplotlib.use("Agg")  # don't plot on screen
        prep.prepare(kwargs)
        facet_inds = list(product(*map(range, prep.ax_arr.shape)))
        for _, row in prep.branch_df.iterrows():
            ax = prep.ax_arr[0, 0]
            if facet is not None:
                i = prep.facet_vals.index(row["facet"])
                ax_i = facet_inds[i]
                ax = prep.ax_arr[ax_i]
                ax.set_title(row["facet"])
            if stratify is not None:
                kwargs["label"] = row["stratify"]
            plot_func(row["branch"], ax=ax, **kwargs)
            if stratify is not None:
                ax.legend()
        if plot_path is not None:
            logging.info(f"saving py figure to {plot_path}")
            prep.fig.savefig(plot_path)
        if show:
            display(prep.fig)
        return prep.fig, prep.ax_arr

    return wrapper


def plot_r(plot_func):
    @wraps(plot_func)
    def wrapper(
        branch: "DataBranch",
        stratify: Optional[str] = None,
        facet: Optional[str] = None,
        plot_path: Optional[AnyStr] = None,
        show: bool = True,
        **kwargs,
    ):
        def _get_plot_script():
            for _plot_source in PlotMethods.R_PLOT_SOURCES:
                _r_script = _plot_source / (plot_func.__name__ + ".R")
                if _r_script.exists():
                    return _plot_source, _r_script

        stratify = None if stratify in PlotPreparator.NONE_VARIANTS else stratify
        facet = None if facet in PlotPreparator.NONE_VARIANTS else facet
        plot_path = plot_path if plot_path else "/tmp/plot.png"

        if facet is not None:
            logging.warning(f"{plot_func.__name__} facet not yet implemented for R plots")
        if stratify is not None:
            logging.warning(f"{plot_func.__name__} stratify not yet supported for R plots")
        plot_source, r_script = _get_plot_script()
        plot_size = kwargs.pop("plot_size", PlotPreparator.DEFAULT_PLOT_RESOLUTION_PX)
        if stratify is not None:
            if stratify in branch.meta:  # col exists in metadata
                kwargs["group.by"] = stratify
            else:
                logging.warning(f"{plot_func.__name__} with key '{stratify}' is skipped because key is not in metadata")
                return
        subset_vals = [None]
        if facet is not None:
            subset_vals = sorted(branch.meta[facet].unique())
        img_arr = []

        for val in subset_vals:
            # corresponding arguments in r/plot_entry_point.R
            args = [
                plot_source,  # r_plot_scripts_path
                branch.paths["root"],  # root_dir
                branch.spec.shell_str,  # spec_str
                facet,  # subset_key
                val,  # subset_val
                branch.current_process,  # current_process
                plot_path,  # plot_file_path
                plot_size[0],  # plot_width_px
                plot_size[1],  # plot_height_px
                json.dumps(
                    "kwargs = " + str(kwargs if kwargs else {})
                ),  # TODO-QC: is there a better way to handle this?
            ]
            logging.info(f"saved R figure to {plot_path}")
            plot_func(branch, r_script, args)  # plot_kwargs already included in args
            img = Image(plot_path)
            if not Path(plot_path).exists():
                logging.warning(f"{plot_func.__name__} output file not found after execution for {val}")
            else:
                if show:
                    display(img)
                img_arr.append(img)
        return img_arr

    return wrapper


# noinspection PyPep8Naming
class requires:
    def __init__(self, req_process):
        self._req_process = req_process

    def __call__(self, func):
        func._requires = self._req_process

        @wraps(func)
        def wrapper(branch: "DataBranch", *args, **kwargs):
            if not branch[self._req_process].success:
                raise ValueError(f"`{func.__name__}` requires a complete and successful process: `{self._req_process}`")
            precursors = branch.spec.get_precursors_lookup(incl_current=True)[branch.current_process]
            if self._req_process not in precursors:
                proc = self._req_process
                raise ValueError(
                    f"This plot method requires a branch at `{proc}` or later. Current process run: {precursors}. If "
                    f"`{proc}` has already been run, please use `branch.goto_process`. Otherwise, please run `{proc}`."
                )
            return func(branch, *args, **kwargs)

        return wrapper