from pathlib import Path
from typing import Union, Optional, List, Dict

from dataforest.core.PlotMethods import PlotMethods


class DataBase:
    """
    Mixin for `DataTree`, `DataBranch`, and derived class
    """

    def __init__(self):
        self.plot = PlotMethods(self)

    @staticmethod
    def _combine_datasets(
        root: Union[str, Path],
        metadata: Optional[Union[str, Path]] = None,
        input_paths: Optional[List[Union[str, Path]]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")
