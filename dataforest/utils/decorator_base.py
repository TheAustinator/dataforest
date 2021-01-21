from functools import update_wrapper, wraps
from typing import Callable, Optional, List, Tuple, Union


# noinspection PyPep8Naming
class decorator_base:
    """
    A decorator base that allows the decorator to be used with or without arguments
    """

    def __init__(self, *args, **kwargs):
        """
        Args:
            *args: wrapped function if no args passed to decorator, otherwise,
                decorator args
            **kwargs: decorator kwargs
        """
        # case 1 no args passed to decorator
        if args and callable(args[0]):
            self._no_args = True
            self._args = tuple()
            self._kwargs = dict()
            self._func = args[0]
            update_wrapper(self, self._func)
        # case 2 args passed to decorator
        else:
            self._no_args = False
            self._args = args
            self._kwargs = kwargs

    def __call__(self, *args, **kwargs):
        """
        If decorator args, this call will be the wrapping of the function,
        whereas if no decorator args, this will be the call of the wrapped
        function
        """
        if self._no_args:
            wrap = self._wrap(self._func)
            return wrap(*args, **kwargs)
        func = args[0]
        sig_wrapper = wraps(func)
        wrap = sig_wrapper(self._wrap(func))
        return wrap

    def _wrap(self, func):
        raise NotImplementedError()
