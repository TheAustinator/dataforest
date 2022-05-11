import logging
from itertools import product
from typing import Union, Literal

import dash_bio as dashbio
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from pandas.core.dtypes.common import is_numeric_dtype, is_bool_dtype
from seaborn import heatmap, clustermap, hls_palette, color_palette


def pairwise_metric(args_map: Union[pd.Series, dict], metric, set_diag=False):
    if isinstance(args_map, dict):
        args_map = pd.Series(args_map)
        dupes = args_map[args_map.index.duplicated()].index.tolist()
        args_map = args_map[~args_map.index.duplicated()]
        if dupes:
            logging.warning(f"duplicates in `args_map`: {dupes}. Dropped dupes")
    pair_keys = product(args_map.index.tolist(), args_map.index.tolist())
    df = pd.DataFrame()
    for keys in pair_keys:
        pair = args_map[list(keys)].tolist()
        result = metric(*pair)
        df.loc[keys[0], keys[1]] = result
    if set_diag is not False:
        dff = df.copy()
        dff.values[[np.arange(df.shape[0])]*2] = 0
        diag_lookup = {True: 0, "max": dff.max().max()}
        diag = diag_lookup.get(set_diag, set_diag)
        df.values[[np.arange(df.shape[0])]*2] = diag
    return df


def pairwise_metric_groups(args_dict_1, args_dict_2, metric):
    """For when there are two separate groups to compare"""
    raise NotImplementedError()


def pairwise_metric_heat(args_dict, metric, cluster=False, branch_cut_row=None, branch_cut_col=None, set_diag=False, engine: Literal["seaborn", "plotly"] = "seaborn", **kwargs):
    plot_engines = {"seaborn": heat_sns, "plotly": heat_plotly}
    try:
        corr = pairwise_metric(args_dict, metric, set_diag=set_diag)
    except __ as e:
        pairwise_metric(pd.Series(args_dict).map(lambda x: np.array(x).reshape(1, -1)), metric, set_diag=set_diag)
    plot_func = plot_engines[engine]
    fig = plot_func(corr, cluster=cluster, branch_cut_row=branch_cut_row, branch_cut_col=branch_cut_col, **kwargs)
    return corr, fig


def heat_plotly(corr):
    fig = dashbio.Clustergram(corr)
    return fig


def heat_sns(corr, cluster=False, branch_cut_row=None, branch_cut_col=None, auto_figsize=0.28, **kwargs):
    if corr.dtypes.iloc[0] == "O":
        corr = corr.astype(int)
    cmap_indices = {"row_colors": corr.index, "col_colors": corr.columns}
    plot_func = clustermap if cluster else heatmap
    row_colors = kwargs.pop("row_colors", None)
    col_colors = kwargs.pop("col_colors", None)
    if auto_figsize and not kwargs.get("figsize", None):
        constant = 8 if cluster else 0
        text_factor = pd.concat([corr.index.to_series(), corr.columns.to_series()])
        text_factor = text_factor.map(str).map(len).max() * auto_figsize / 20
        scale = lambda n: (n + constant) * auto_figsize + text_factor
        kwargs["figsize"] = tuple(map(scale, corr.shape[::-1]))
    if not cluster and kwargs.get("figsize", None):
        _, kwargs["ax"] = plt.subplots(figsize=kwargs.pop("figsize"))
    for k, arr in {"row_colors": row_colors, "col_colors": col_colors}.items():
        if arr is None:
            continue
        if is_numeric_dtype(arr) and not is_bool_dtype(arr):
            cmap = color_palette("mako", as_cmap=True)
            arr = arr / arr.max()
        else:
            cmap = dict(zip(set(arr), hls_palette(len(set(arr)), l=0.5, s=0.8)))
        arr = pd.Series(arr, index=cmap_indices[k])
        if arr.dtype.name == "category":
            if arr.str.isnumeric().all():
                arr = arr.astype(float)
            else:
                arr = arr.astype(str)
        kwargs[k] = arr.map(cmap)
    clusters = dict()
    if branch_cut_col:
        clusters["col"], kwargs["col_colors"] = _branch_cut(corr.T, branch_cut_col)
    if branch_cut_row:
        clusters["row"], kwargs["row_colors"] = _branch_cut(corr, branch_cut_row)
    fig = plot_func(corr, **kwargs)
    fig.clusters = clusters
    return fig


def _branch_cut(corr, cut):
    import scipy.cluster.hierarchy as spc
    pdist = spc.distance.pdist(corr)
    linkage = spc.linkage(pdist, method='complete')
    cluster_ids = spc.fcluster(linkage, cut * pdist.max(), 'distance')
    cluster_ids = list(map(str, cluster_ids))
    lut = dict(zip(set(cluster_ids), hls_palette(len(set(cluster_ids)), l=0.5, s=0.8)))
    cluster_ids = pd.Series(cluster_ids, index=corr.index, name="cluster");
    return cluster_ids, cluster_ids.map(lut)
