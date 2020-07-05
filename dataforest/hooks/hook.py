from functools import partial, update_wrapper, wraps
from typing import Callable, Optional, List, Tuple, Union


# noinspection PyPep8Naming
class hook:
    def __init__(self, attrs=tuple()):
        # case where no attributes
        if callable(attrs):
            update_wrapper(self, attrs)
            self._func = attrs
            self._no_attrs = True
        else:
            self._no_attrs = False
            self._attrs = attrs

    def __call__(self, func):
        # case where func is actually dataprocess b/c no attributes
        if self._no_attrs:
            wrap = self._wrap(self._func)
            return wrap(func)
        return self._wrap(func)

    def _wrap(self, func):
        def wrapper(dp):
            attrs = getattr(self, "attrs", [])
            if all([hasattr(dp.forest, attr) for attr in attrs]):
                return func(dp)

        if self._no_attrs:
            update_wrapper(self, func)
        else:
            wrapper = wraps(func)(wrapper)
        return wrapper
