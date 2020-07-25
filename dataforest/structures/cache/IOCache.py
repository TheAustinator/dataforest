from typing import Dict, Callable

from dataforest.structures.cache.HashCash import HashCash
from dataforest.filesystem.io.FileIO import FileIO


class IOCache(HashCash):
    """
    Lazy loading for `FileIO`s for a single process.

    Key: `file_alias` -- alias for filename (union of keys in `standard_files`
        and `file_map[process_name]` from dataforest.config.default_config.yaml

    Val: `FileIO` object for `file_alias`
    """

    def __init__(
        self,
        file_dict: Dict[str, str],
        method_dict: Dict[str, Callable],
        kwargs_dict: Dict[str, dict],
        path_cache: "PathCache",
    ):
        super().__init__()
        self._file_dict = file_dict
        self._method_dict = method_dict
        self._kwargs_dict = kwargs_dict
        self._path_cache = path_cache

    def _get(self, file_alias):
        filepath = self._path_cache[file_alias]
        method = self._method_dict[file_alias]
        method_kwargs = self._kwargs_dict[file_alias]
        file_io = FileIO(filepath, method, method_kwargs)
        return file_io
