from functools import wraps
from typing import Any, Callable, Iterable

import pandas as pd


class DistributedContainer(list):
    _TETHER_EXCLUDE = {
        "__new__",
        "__class__",
        "__init__",
        "__weakref__",
        "__dict__",
        "__getitem__",
        "__setitem__",
        "__setattribute__",
        "__getattribute__",
        "__bool__",
    }
    ELEM_CLASS = None

    def __init__(self, data_list: Iterable[Any]):
        super().__init__(list(data_list))
        self._check_elem_type()

    def get_elem(self, i: int):
        """
        Get the df or series from the list-like structure since getitem is
        overloaded by the pandas method. The getitem method will return a
        list of results from getitem being applied to each element, whereas
        this method can be used to get the elements themselves.
        Args:
            i: index in list-like structure
        """
        return list.__getitem__(self, i)

    @staticmethod
    def _get_elem_type(container):
        if not container:
            raise ValueError(f"container must not be empty. Got: {container}")
        elem_type = container[0].__class__
        if not all(isinstance(x, elem_type) for x in container):
            import ipdb

            ipdb.set_trace()
            raise TypeError(f"container requires a single type. Got: {[type(x) for x in container]}")
        return elem_type

    def _check_elem_type(self):
        if not self._get_elem_type(self) == self.ELEM_CLASS:
            raise TypeError(
                f"`data_list` for {self.__class__} must contain only {self.ELEM_CLASS}. Got: {[type(x) for x in self]}"
            )

    def __getitem__(self, item):
        return self._getitem(item)

    def __setitem__(self, key, value):
        return self._setitem(key, value)


class DistributedSeries(DistributedContainer):
    ELEM_CLASS = pd.Series


class DistributedDataFrame(DistributedContainer):
    ELEM_CLASS = pd.DataFrame


ELEM_TYPE_DICT = {
    pd.Series: DistributedSeries,
    pd.DataFrame: DistributedDataFrame,
}


def distribute_methods(cls):
    names = set(filter(lambda s: not s.startswith("__"), dir(cls.ELEM_CLASS)))
    for name in names:
        distributed_method = _build_distributed_method(cls, name)
        setattr(cls, name, distributed_method)
    cls._getitem = _build_distributed_method(cls, "__getitem__")
    cls._setitem = _build_distributed_method(cls, "__setitem__")


def _build_distributed_method(cls, method_name) -> Callable:
    obj_method = getattr(cls.ELEM_CLASS, method_name)

    def _get_method_wrapper(_obj_method):
        if isinstance(obj_method, property):
            return property
        elif not hasattr(obj_method, "__self__"):
            return staticmethod
        elif getattr(obj_method, "__self__", None) is cls.ELEM_CLASS:
            return classmethod

    method_wrapper = _get_method_wrapper(obj_method)

    @wraps(obj_method)
    def _distributed_method(_self, *args, **kwargs):
        def _split_if_container(i, arg):
            if isinstance(arg, DistributedContainer):
                return arg.get_elem(i)
            return arg

        def _single_method_kernel(i, obj):
            args_ = [_split_if_container(i, arg) for arg in args]
            kwargs_ = {k: _split_if_container(i, v) for k, v in kwargs.items()}
            if method_wrapper is classmethod:
                args_.insert(0, obj.__class__)
            if not method_wrapper:
                args_.insert(0, obj)
            return obj_method(*args_, **kwargs_)

        def _single_property_kernel(_, obj):
            prop = getattr(obj, method_name)
            return prop

        def _distributed_kernel():
            ret = list()
            for i, obj in enumerate(_self):
                if method_wrapper is property:
                    val = _single_property_kernel(i, obj)
                else:
                    val = _single_method_kernel(i, obj)
                ret.append(val)
            elem_type = _self._get_elem_type(ret)
            return ELEM_TYPE_DICT[elem_type](ret)

        return _distributed_kernel()

    if method_wrapper is property:
        _distributed_method = method_wrapper(_distributed_method)

    return _distributed_method


distribute_methods(DistributedSeries)
distribute_methods(DistributedDataFrame)
