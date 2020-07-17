from dataforest.structures.cache.HashCache import HashCash
from dataforest.filesystem.io.FileIO import FileIO


class IOCache(HashCash):
    def __init__(self, file_dict, method_dict, kwargs_dict, path_cache):
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
