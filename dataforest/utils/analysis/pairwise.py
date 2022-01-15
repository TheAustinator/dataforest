from itertools import product
from typing import Union

import pandas as pd
from seaborn import heatmap, clustermap


def pairwise_metric(args_map: Union[pd.Series, dict], metric):
    if isinstance(args_map, dict):
        args_map = pd.Series(args_map)
    pair_keys = product(args_map.index.tolist(), args_map.index.tolist())
    df = pd.DataFrame()
    for keys in pair_keys:
        pair = args_map[list(keys)].tolist()
        result = metric(*pair)
        df.loc[keys[0], keys[1]] = result
    return df


def pairwise_metric_groups(args_dict_1, args_dict_2, metric):
    """For when there are two separate groups to compare"""
    raise NotImplementedError()


def pairwise_metric_heat(args_dict, metric, cluster=False, **kwargs):
    plot_func = clustermap if cluster else heatmap
    corr = pairwise_metric(args_dict, metric)
    return corr, plot_func(corr, **kwargs)
