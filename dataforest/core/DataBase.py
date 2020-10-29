from abc import ABC
from pathlib import Path
from typing import Union, Optional, List, AnyStr

from dataforest.core.PlotMethods import PlotMethods


class DataBase:
    """
    Mixin for `DataTree`, `DataBranch`, and derived class
    """

    _PLOT_METHODS = PlotMethods

    def __init__(self):
        self.root = None
        self.plot = self._PLOT_METHODS(self)

    @property
    def root_built(self):
        return (Path(self.root) / "meta.tsv").exists()

    @staticmethod
    def _combine_datasets(
        root: AnyStr,
        metadata: Optional[AnyStr] = None,
        input_paths: Optional[List[AnyStr]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")
