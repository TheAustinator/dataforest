from importlib import import_module
import importlib.util
from pathlib import Path
from typing import Optional, Callable


def collect_hooks(path):
    return _collector(path, "hooks.py", _function_filter_hook)


def collect_processes(path):
    return _collector(path, "process.py", _function_filter_process)


def _collector(path, filename: Optional[str] = None, func_filter: Optional[Callable] = None):
    # TODO: fragile to windows -- test
    funcs = dict()
    if "/" in str(path) in path:
        module_files = _get_module_files(path, filename)
    else:
        module = import_module(str(path))
        module_files = _get_module_files(module.__file__, filename)
    for path in module_files:
        module = _get_module_from_filepath(str(path))
        if "__all__" in module.__dict__:
            names = module.__dict__["__all__"]
        else:
            names = _filter_private(module.__dict__).keys()
        module_funcs = {k: getattr(module, k) for k in names}
        module_funcs = {k: v for k, v in module_funcs.items() if func_filter(v)}
        funcs.update(module_funcs)
    return funcs


def _get_module_files(path, filename: Optional[str] = None):
    path = Path(path)
    path = path.parent if path.stem == "__init__" else path
    if filename:
        file_filter = lambda p: p.name == filename
    else:
        file_filter = lambda p: True
    filepaths = list(filter(file_filter, path.rglob("*.py")))
    return filepaths


def _get_module_from_filepath(path):
    spec = importlib.util.spec_from_file_location("module.name", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _filter_private(dict_):
    return {k: v for k, v in dict_.items() if not k.startswith("_")}


def _function_filter_hook(func):
    return func.__name__.startswith("hook_")


def _function_filter_process(func):
    if hasattr(func, "__closure__"):
        if func.__closure__ is not None:
            for closure in func.__closure__:
                if "dataprocess" in closure.cell_contents.__class__.__name__:
                    return True
    return False
