from typing import Any, Callable, Optional, Union

from pathlib import Path


class FileIO:
    """
    Read/write interface to a specified file
    Attributes:
        filepath: path to file
        method: method to read or write to file
        method_kwargs: static keyword arguments for method call
    """

    def __init__(self, filepath: Union[str, Path], method: Callable, method_kwargs: dict):
        # TODO: handle H5
        self.filepath = filepath
        self.method = method
        self.method_kwargs = method_kwargs

    def __call__(self, obj: Optional[Any] = None, **kwargs) -> Any:
        """
        Execute read or write on file
        Args:
            obj: object for writing
            **kwargs: dynamically defined kwargs for read or write, which will
                update static kwargs

        Returns:
            output from read or write method
        """
        kwargs = {**self.method_kwargs, **kwargs}
        if self.method is None:
            raise NotImplementedError()
        args = [self.filepath]
        if obj is not None:
            args.append(obj)
        return self.method(*args, **kwargs)
