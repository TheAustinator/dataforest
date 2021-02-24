from itertools import product

import pandas as pd
from seaborn import heatmap


def pairwise_metric(args_dict, metric):
    def _map_df(results_dict):
        keys = tuple(set.union(*list(map(set, results_dict.keys()))))
        df = pd.DataFrame(columns=keys, index=keys)

        def map_item(item):
            df.loc[item[0][0], item[0][1]] = item[1]

        list(map(map_item, results_dict.items()))
        return df

    results_dict = dict()
    pair_keys = product(args_dict, args_dict)
    for keys in pair_keys:
        pair = list(map(args_dict.__getitem__, keys))
        result = metric(*pair)
        results_dict[keys] = result
    df = _map_df(results_dict).astype(float)
    return df


def pairwise_metric_heat(args_dict, metric, **kwargs):
    return heatmap(pairwise_metric(args_dict, metric), **kwargs)
