from functools import wraps
from typing import Any, Dict, Callable

from dataforest.utils.decorator_base import decorator_base


def default_kwargs(defaults: Dict[str, Any]) -> Callable:
    """
    Decorator which provides a set of default keyword arguments for a function,
    to which any keyword arguments passed to the function directly are added.
    Keyword arguments to the function take precedent.
    Args:
        defaults: `dict` of default keyword arguments
    """

    def decorator_default_kwargs(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            kwargs = {**defaults, **kwargs}
            return func(*args, **kwargs)

        return wrapper

    return decorator_default_kwargs


# noinspection PyPep8Naming
class sub_kwargs(decorator_base):
    # TODO: may not need decorator_base, because shouldn't be called without args
    """
    A decorator for functions which have accept dictionaries of kwargs for
    other functions which are called within the function. Replaces `None`
    defaults with empty dicts. Can't use empty dict as default because mutable,
    and can't unpack `None`.
    """

    def _wrap(self, func):
        def wrapper(*args, **kwargs):
            # TODO: deepcopy kwargs?
            sub_kwargs_given = set(self._args).intersection(kwargs)
            sub_kwargs_empty = set(self._args).difference(kwargs)
            for key in sub_kwargs_given:
                val = kwargs[key]
                if not isinstance(val, dict):
                    raise TypeError(f"Must provide dict for sub kwarg `{key}`. Got {val} ({type(val)})")
            for key in sub_kwargs_empty:
                if kwargs.get(key, None) is None:
                    kwargs[key] = dict()
            return func(*args, **kwargs)

        return wrapper
