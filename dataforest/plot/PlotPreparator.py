from math import ceil
from typing import Callable, Optional, Literal, Union, List, Any, Tuple, Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from dataforest.core.DataBranch import DataBranch


class PlotPreparator:
    DEFAULT_PLOT_RESOLUTION_PX = (500, 500)  # width, height in pixels
    NONE_VARIANTS = [None, "none", "None", "NULL", "NA"]
    _DEFAULT_N_COLS = 3

    def __init__(self, branch: "DataBranch"):
        self.branch_df = pd.DataFrame({"branch": [branch]})
        self.ax_arr = None
        self.fig = None
        self.facet_cols = None
        self.strat_cols = None

    @property
    def facet_vals(self):
        if not self.facet_cols:
            return None
        return sorted(self.branch_df["facet"].unique())

    @property
    def srat_vals(self):
        if not self.strat_cols:
            return None
        return sorted(self.branch_df["stratify"].unique())

    def prepare(self, plot_kwargs: dict):
        xlim = plot_kwargs.pop("xlim", None)
        ylim = plot_kwargs.pop("ylim", None)
        xscale = plot_kwargs.pop("xscale", None)
        yscale = plot_kwargs.pop("yscale", None)

        @np.vectorize
        def _apply_ax_kwargs(ax_: plt.Axes):
            if xlim:
                ax_.set_xlim(xlim)
            if ylim:
                ax_.set_ylim(ylim)
            if xscale:
                ax_.set_xscale(xscale)
            if yscale:
                ax_.set_yscale(yscale)

        plot_size = plot_kwargs.pop("plot_size", self.DEFAULT_PLOT_RESOLUTION_PX)
        figsize = plot_kwargs.pop("figsize", None)
        ax_arr = plot_kwargs.pop("ax", None)
        fig = plot_kwargs.pop("fig", plt.gcf())
        if self.ax_arr is not None:
            ax_arr = self.ax_arr
        elif ax_arr is None:
            fig, ax_arr = plt.subplots(1, 1)
        if not isinstance(ax_arr, np.ndarray):
            ax_arr = np.array([[ax_arr]])
        elif ax_arr.ndim == 1:
            ax_arr = np.expand_dims(ax_arr, axis=0)
        _apply_ax_kwargs(ax_arr)
        dpi = fig.get_dpi()
        # scale to pixel resolution, irrespective of screen resolution
        fig.set_size_inches(plot_size[0] / float(dpi), plot_size[1] / float(dpi))
        if figsize:
            fig.set_size_inches(*figsize)
        self.fig = fig
        self.ax_arr = ax_arr

    def facet(self, cols: Union[str, List[str]], n_rows: Optional[int] = None, n_cols: Optional[int] = None):
        self._branch_groupby(cols, "facet")
        self.facet_cols = cols
        labels = self.branch_df["facet"].unique()
        dim = self._get_facet_dim(labels, n_rows, n_cols)
        _, self.ax_arr = plt.subplots(*dim, sharex="col", sharey="row")

    def stratify(self, cols: Union[str, List[str]], plot_kwargs):
        self._branch_groupby(cols, "stratify")
        self.strat_cols = cols

    def _branch_groupby(self, cols: Union[str, Iterable[str]], key_colname: Literal["facet", "stratify"]):
        df = self.branch_df
        df["grp"] = df["branch"].apply(lambda branch: list(branch.groupby(cols)))
        df = df.explode("grp").reset_index(drop=True)
        df[[key_colname, "branch"]] = pd.DataFrame(df["grp"].tolist(), index=df.index)
        self.branch_df = df

    def _get_facet_dim(
        self, labels: List[Any], n_rows: Optional[int] = None, n_cols: Optional[int] = None
    ) -> Tuple[int, int]:
        """
        Get dimensions of subplot grid for facet
        Args:
            labels: unique labels in facet col
            n_rows:
            n_cols:

        Returns: (n_rows, n_cols)
        """
        # TODO: if two cols and rows/cols within range, have option to use each as an axis
        if not n_rows and not n_cols:
            n_cols = min(len(labels), self._DEFAULT_N_COLS)
        if n_rows and n_cols and n_rows != len(labels) / n_cols:
            raise ValueError(
                f"If both `n_rows` and `n_cols` are specified, their product "
                f"must be appropriate for ({len(labels)})"
            )
        if n_cols:
            n_rows = ceil(len(labels) / n_cols)
        else:
            n_cols = ceil(len(labels) / n_rows)
        return n_rows, n_cols
