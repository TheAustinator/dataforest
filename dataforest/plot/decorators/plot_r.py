import json
import logging
from pathlib import Path
from typing import Optional, AnyStr, Callable, TYPE_CHECKING

from IPython.core.display import Image, display

from dataforest.core.PlotMethods import PlotMethods
from dataforest.plot.PlotPreparator import PlotPreparator
from dataforest.plot.decorators.plot_base import plot_base

if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch


class plot_r(plot_base):
    def _wrap(self, plot_func: Callable):
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
                    logging.warning(
                        f"{plot_func.__name__} with key '{stratify}' is skipped because key is not in metadata"
                    )
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
