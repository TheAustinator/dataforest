from functools import wraps
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd


def set_prop(inst, name, method):
    # TODO: make staticmethod
    cls_ = type(inst)
    if not hasattr(cls_, "__perinstance"):
        cls = type(cls_.__name__, (cls_,), {})
        cls.__perinstance = True
        cls.__parent = cls_
        inst.__class__ = cls
    else:
        cls = cls_
    setattr(cls, name, method)


class DistributedContainer(list):
    TETHER_EXCLUDE = {
        "__class__",
        "__init__",
        "__weakref__",
        "__dict__",
        "__getitem__",
        "__setitem__",
        # "__getattr__",
        # "__setattr__",
    }
    MANUAL_ATTRS = ["_inferred_dtype"]
    ELEM_CLASSES = [
        pd.DataFrame,
        pd.Series,
        pd.Index,
        pd.core.strings.StringMethods,
        pd.core.arrays.categorical.CategoricalAccessor,
    ]
    EQ_CHECKS = [
        lambda arr: all(x == arr[0] for x in arr),
        lambda arr: all(arr[0].equals(x) for x in arr),
        lambda arr: all(np.array_equal(arr[0], x) for x in arr),
    ]

    def __init__(self, data_list: Iterable[Any]):
        super().__init__(list(data_list))
        self._elem_class = self._get_elem_class(self)
        self._tether_df_methods()

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

    @classmethod
    def _get_elem_class(cls, container):
        for class_ in cls.ELEM_CLASSES:
            if all(isinstance(x, class_) for x in container):
                return class_

    def _tether_df_methods(self):
        if self._elem_class is None:
            return
        names = set(dir(self._elem_class)).difference(self.TETHER_EXCLUDE)
        for name in names:
            distributed_method = self._build_distributed_method(name)
            if isinstance(distributed_method, property):
                set_prop(self, name, distributed_method)
            else:
                try:
                    setattr(self, name, distributed_method)
                except Exception as e:
                    raise e
        self._getitem = self._build_distributed_method("__getitem__")
        if hasattr(self._elem_class, "__setitem__"):
            self._setitem = self._build_distributed_method("__setitem__")

    def _build_distributed_method(self, method_name) -> Callable:
        obj_method = getattr(self._elem_class, method_name)
        manual_attrs = {name: getattr(obj_method, name) for name in self.MANUAL_ATTRS if hasattr(obj_method, name)}

        def _get_method_wrapper(_obj_method):
            if isinstance(obj_method, property):
                return property
            elif isinstance(obj_method, type):
                return type
            elif not hasattr(obj_method, "__self__"):
                return staticmethod
            elif getattr(obj_method, "__self__", None) is self._elem_class:
                return classmethod

        method_wrapper = _get_method_wrapper(obj_method)

        @wraps(obj_method)
        def _distributed_method(*args, **kwargs):
            def _split_args(i, arg):
                if isinstance(arg, DistributedContainer):
                    return arg.get_elem(i)
                return arg

            def _callable_kernel(i, obj):
                args_ = [_split_args(i, arg) for arg in args]
                kwargs_ = {k: _split_args(i, v) for k, v in kwargs.items()}
                return obj_method(obj, *args_, **kwargs_)

            def _attr_kernel(_, obj):
                prop = getattr(obj, method_name)
                return prop

            def _check_all_same(arr):
                for eq_check in self.EQ_CHECKS:
                    try:
                        return eq_check(arr)
                    except Exception as e:
                        pass
                raise e

            def _distributed_kernel():
                kernel = _callable_kernel if callable(obj_method) else _attr_kernel
                ret = [kernel(*x) for x in enumerate(self)]
                all_same = _check_all_same(ret)
                ret = ret[0] if all_same else self._cls(ret)
                return ret

            return _distributed_kernel()

        if method_wrapper is type:
            _distributed_method = self._cls([getattr(x, method_name, None) for x in self])
        if method_wrapper is property:
            _distributed_method = property(_distributed_method)
        return _distributed_method

    @property
    def _cls(self):
        return getattr(self.__class__, "__parent", self.__class__)

    def __getitem__(self, item):
        return self._getitem(item)

    def __setitem__(self, key, value):
        return self._setitem(key, value)
