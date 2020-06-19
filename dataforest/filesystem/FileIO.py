from typing import Any, Callable, Optional, Union

from pathlib import Path


class FileIO:
    """
    Read/write interface to a specified file
    Attributes:
        filepath: path to file
        reader: method to read file
        writer: method to write to file
        reader_kwargs: keyword arguments for reader
        writer_kwargs: keyword arguments for writer
    """

    def __init__(
            self,
            filepath: Union[str, Path],
            reader: Callable,
            writer: Optional[Callable],
            reader_kwargs: dict,
            writer_kwargs: dict,
    ):
        # TODO: handle H5
        self.filepath = filepath
        self.reader = reader
        self.writer = writer
        self.reader_kwargs = reader_kwargs
        self.writer_kwargs = writer_kwargs

    def write(self, obj: Any, **kwargs):
        """
        Write `obj` to `filepath` using `self.writer` and passing
        `writer_kwargs` with any additional `kwargs`
        Args:
            obj: object to write
            **kwargs: additional keyword arguments for writer

        Returns:
            output from `writer`
        """
        # TODO: add makedirs functionality
        kwargs = {**self.writer_kwargs, **kwargs}
        if self.writer is None:
            raise NotImplementedError()
        return self.writer(self.filepath, obj, **kwargs)

    def read(self, **kwargs):
        """
        Read from `filepath` using `self.reader` and passing
        `reader_kwargs` with any additional `kwargs`
        Args:
            **kwargs: additional keyword arguments for reader

        Returns:
            output from `reader`
        """
        kwargs = {**self.reader_kwargs, **kwargs}
        if self.reader is None:
            raise NotImplementedError()
        return self.reader(self.filepath, **kwargs)
