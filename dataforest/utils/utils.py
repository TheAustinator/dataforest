from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Dict, Union

import pandas as pd


def update_recursive(dict_: dict, other: Mapping, inplace: bool = False) -> dict:
    """

    Args:
        dict_: `dict` which is to updated by other
        other: `dict` which is to be merged into `dict_`
        inplace: whether to update `dict_` inplace or make a copy

    Returns:
        dict_: recursively updated dict_
    """
    if not inplace:
        dict_ = deepcopy(dict_)
    for k, v in other.items():
        if isinstance(v, Mapping):
            dict_[k] = update_recursive(dict_.get(k, {}), v, inplace)
        else:
            dict_[k] = v
    return dict_


def node_lineage_lookup(dict_: Dict[Any, Union[dict, set]]) -> Dict[Any, list]:
    """
    Build a traversal lookup to get to each node name in a nested `dict`.
    Example:
        input: {'a':
                  {'b': {'c'}}
               }
        output: {
                   'a': [],
                   'b': ['a'],
                   'c': ['a', 'b'],
                }
    Args:
        dict_: input nested dict  in which all keys are either `dict` or `set`

    Returns:
        node_lineage: lookup with node names as keys and values as lists of
            keys in `dict_` to reach the node

    # TODO: use lists or sets to handle duplicates
    # TODO: allow non-set values
    """
    node_lineage = {}

    def _helper(sub_dict, stack):
        nonlocal node_lineage
        for node in sub_dict:
            if node in node_lineage:
                raise NotImplementedError(
                    f"No duplicates allowed in process hierarchy: {node}"
                )
            node_lineage[node] = stack.copy()
            val = sub_dict[node]
            if val is None:
                continue
            elif isinstance(val, dict):
                _helper(val, stack.copy() + [node])
            elif isinstance(val, set):
                for x in val:
                    if x in node_lineage:
                        raise ValueError(
                            f"No duplicates allowed in process hierarchy: {x}"
                        )
                    node_lineage[x] = stack.copy() + [node]
            else:
                raise TypeError()

    _helper(dict_, [])
    return node_lineage


def label_df_partitions(
        df: pd.DataFrame, columns: Any, encodings: bool = False
) -> pd.DataFrame:
    if not isinstance(columns, (list, tuple, set)):
        columns = (columns,)
    columns = sorted(list(columns))
    if columns:
        df["partition"] = list(map(tuple, df[columns].values))
        df["partition"] = df["partition"].astype("category")
        if encodings:
            df["partition_code"] = df["partition"].apply(lambda x: "_".join(x))
            # df["partition_code"] = df["partition"].cat.codes
    return df
