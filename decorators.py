from functools import wraps
from typing import Any, Dict, Callable


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
