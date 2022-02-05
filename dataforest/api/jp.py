from typing import Iterable, Union

from IPython.display import display
from ipywidgets import Checkbox


def checkboxes(names: Union[str, Iterable[str]]):
    names = [names, ] if isinstance(names, str) else names
    changed = lambda b: print(b)
    boxes = [Checkbox(False, description=x) for x in names]
    [display(b) for b in boxes]
    [b.observe(changed) for b in boxes]