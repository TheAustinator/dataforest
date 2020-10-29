from importlib import import_module
from pathlib import Path
from typing import AnyStr, List


def get_module_paths(paths: List[AnyStr]) -> List[Path]:
    def _get_module_path(path: AnyStr) -> Path:
        module = import_module(str(path))
        path = Path(module.__file__)
        if path.stem == "__init__":
            path = path.parent
        return path

    return list(map(_get_module_path, paths))
