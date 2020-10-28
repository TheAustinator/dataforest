from functools import update_wrapper, wraps
from typing import Callable, TYPE_CHECKING

from dataforest.utils.decorator_base import decorator_base

if TYPE_CHECKING:
    from dataforest.hooks import dataprocess


# noinspection PyPep8Naming
class hook(decorator_base):
    def _wrap(self, func: Callable):
        def wrapper(dp: "dataprocess"):
            attrs = getattr(self, "_args", [])
            if all([hasattr(dp, attr) for attr in attrs]):
                return func(dp)

        return wrapper
