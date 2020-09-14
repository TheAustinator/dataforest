from pathlib import Path
from typing import Union

from IPython.display import Image


class PlotIO:
    def __init__(self, filepath: Union[str, Path]):
        self.filepath = filepath

    def show(self):
        Image(filename=self.filepath)
