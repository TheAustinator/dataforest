import logging
from itertools import product
from typing import Optional, AnyStr, Union, Tuple, Callable, TYPE_CHECKING

import matplotlib
import numpy as np
from IPython.core.display import display
from matplotlib import pyplot as plt

from dataforest.plot.PlotPreparator import PlotPreparator
from dataforest.plot.decorators.plot_base import plot_base

if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch


class plot_py(plot_base):
    def _wrap(self, plot_func: Callable):
        def wrapper(
            branch: "DataBranch",
            stratify: Optional[str] = None,
            facet: Optional[str] = None,
            plot_path: Optional[AnyStr] = None,
            facet_dim: tuple = (),
            leg_alpha: int = 1,
            leg_s: int = 36,
            show: bool = True,
            **kwargs,
        ) -> Union[plt.Figure, Tuple[plt.Figure, np.ndarray]]:
            # bypass_kwargs = {k: v for k, v in kwargs.items() if k in self._bypass}
            # kwargs = {k: v for k, v in kwargs.items() if k not in self._bypass}
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
                # kwargs = {**kwargs, **bypass_kwargs}
                plot_func(row["branch"], ax=ax, **kwargs)
                if stratify is not None:
                    leg = ax.legend()
                    for lh in leg.legendHandles:
                        lh.set_alpha(leg_alpha)
                        lh._sizes = [leg_s]
            if plot_path is not None:
                logging.info(f"saving py figure to {plot_path}")
                prep.fig.savefig(plot_path)
            if show:
                display(prep.fig)
            return prep.fig, prep.ax_arr

        return wrapper
