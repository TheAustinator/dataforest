from functools import wraps
from typing import TYPE_CHECKING, Callable, List, Iterable, Union

import pandas as pd

from dataforest.structures.cache.BranchCache import BranchCache

if TYPE_CHECKING:
    from dataforest.core.DataTree import DataTree


class DataFrameList(list):
    TETHER_EXCLUDE = {"__class__", "__init__", "__weakref__", "__dict__", "__getitem__", "__setitem__"}

    def __init__(self, df_list: Iterable[Union[pd.DataFrame, pd.Series]]):
        super().__init__(list(df_list))
        self._elem_class = self._get_elem_class(self)
        self._tether_df_methods()

    def get_elem(self, i: int):
        """
        Get the df or series from the list-like structure since getitem is
        overloaded by the pandas method
        Args:
            i: index in list-like structure
        """
        return list.__getitem__(self, i)

    @staticmethod
    def _get_elem_class(container):
        if all(isinstance(x, pd.DataFrame) for x in container):
            return pd.DataFrame
        elif all(isinstance(x, pd.Series) for x in container):
            return pd.Series

    def _tether_df_methods(self):
        if self._elem_class is None:
            return
        names = set(dir(self._elem_class)).difference(self.TETHER_EXCLUDE)
        for name in names:
            distributed_method = self._build_distributed_method(name)
            setattr(self, name, distributed_method)
        self._getitem = self._build_distributed_method("__getitem__")
        self._setitem = self._build_distributed_method("__setitem__")

    def _build_distributed_method(self, method_name) -> Callable:
        df_method = getattr(self._elem_class, method_name)

        @wraps(df_method)
        def _distributed_method(*args, **kwargs):
            def _split_args(i, arg):
                if isinstance(arg, self.__class__):
                    return arg.get_elem(i)
                return arg

            def _single_kernel(i, df):
                args_ = [_split_args(i, arg) for arg in args]
                kwargs_ = {k: _split_args(i, v) for k, v in kwargs.items()}
                return df_method(df, *args_, **kwargs_)

            def _distributed_kernel():
                ret = [_single_kernel(*x) for x in enumerate(self)]
                return self.__class__(ret)

            return _distributed_kernel()

        return _distributed_method

    def __getitem__(self, item):
        return self._getitem(item)

    def __setitem__(self, key, value):
        return self._setitem(key, value)
