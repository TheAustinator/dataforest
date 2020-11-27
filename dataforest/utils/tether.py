from functools import wraps
from typing import Any, Iterable, Optional, Callable, List

from dataforest.utils.copy_func import copy_func


def tether(obj, tether_attr, incl_methods=None, excl_methods=None):
    """
    Tether a static method to a class instance, supplying a specified
    `tether_arg` as the first argument, implicitly.
    """
    if incl_methods and excl_methods:
        ValueError("Cannot specify both `incl_methods` and `excl_methods`")
    tether_arg = getattr(obj, tether_attr)
    method_list = get_methods(obj, excl_methods, incl_methods)
    for method_name in method_list:
        method = copy_func(getattr(obj, method_name))
        tethered_method = make_tethered_method(method, tether_arg)
        setattr(obj, method_name, tethered_method)


def get_methods(
    obj: Any, excl_methods: Optional[Iterable[str]] = None, incl_methods: Optional[List[str]] = None
) -> List[str]:
    """Get all user defined methods of an object"""
    all_methods = filter(lambda x: not x.startswith("__"), dir(obj))
    method_list = []
    if incl_methods:
        return incl_methods
    for method_name in all_methods:
        if hasattr(obj, method_name):
            if callable(getattr(obj, method_name)):
                method_list.append(method_name)
    if excl_methods:
        method_list = [m for m in method_list if m not in excl_methods]
    return method_list


def make_tethered_method(method: Callable, tether_arg: str) -> Callable:
    """
    Returns a callable which no longer requires first positional arg because it
    the enclosing objects attribute named by `tether_arg` is passed implicitly.
    """

    @wraps(method)
    def tethered_method(*args, **kwargs):
        return method(tether_arg, method.__name__, *args, **kwargs)

    return tethered_method
