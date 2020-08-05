from pathlib import Path
from typing import Union, Optional, List


class DataBase:
    """
    Mixin for `DataTree`, `DataBranch`, and derived class
    """

    @staticmethod
    def _combine_datasets(
        root: Union[str, Path],
        metadata: Optional[Union[str, Path]] = None,
        input_paths: Optional[List[Union[str, Path]]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")
