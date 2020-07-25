from functools import update_wrapper
from types import FunctionType
from typing import Callable


def copy_func(f: Callable) -> Callable:
    """Duplicate a function object"""
    g = FunctionType(f.__code__, f.__globals__, name=f.__name__, argdefs=f.__defaults__, closure=f.__closure__)
    g = update_wrapper(g, f)
    g.__kwdefaults__ = f.__kwdefaults__
    return g
