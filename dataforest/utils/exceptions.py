import logging
import traceback
from typing import Dict


class TraversalError(ValueError):
    def __init__(self, tree, stack):
        self._tree = tree
        self._stack = stack

    def __str__(self):
        return f"Cannot access stack: {self._stack} in tree: {self._tree}"


class AscensionError(TraversalError):
    def __str__(self):
        return f"attempted to ascend beyond root of tree: {self._tree}"


class InputDataNotFound(FileNotFoundError):
    def __init__(self, branch, requires, process_name):
        self._branch = branch
        self._requires = requires
        self._process_name = process_name

    def __str__(self):
        return (
            f"Process `{self._process_name}` requires input data from `{self._requires}`. Not found at expected"
            f"location: {self._branch.paths_exists[self._requires]} based on `branch.spec`: {self._branch.spec}"
        )


class UnnecessaryKeysError(KeyError):
    def __init__(self, dict_, process_name: str = None):
        self.dict = dict_
        self.process_name = process_name

    def __str__(self):
        end_str = f" - {self.dict}. Please remove unnecessary keys from `DataBranch`."
        if self.process_name:
            str_ = f"Unnecessary key(s) for `process_name`: {self.process_name} "
        else:
            str_ = f"Unnecessary key(s) "
        return str_ + end_str


class BadOperation(KeyError):
    OPERATION = None

    def __init__(self, column, val):
        self._column = column
        self._val = val

    def __str__(self):
        return (
            f"{self.OPERATION} resulted in no rows - column: {self._column} val: {self._val}. Please note that any "
            f"spaces in `spec` should be converted to underscores!"
        )


class BadSubset(BadOperation):
    OPERATION = "_SUBSET_"


class BadFilter(BadOperation):
    OPERATION = "_FILTER_"


class HookException(Exception):
    def __init__(self, process_name: str, child_exceptions: Dict[str, Exception]):
        self.process_name = process_name
        self.child_exceptions = child_exceptions

    def __str__(self):
        str_ = str(self.child_exceptions)
        for i, (hook_name, e) in enumerate(self.child_exceptions.items()):
            str_ += "\n\n" + f"HOOK EXCEPTION {i} ({self.process_name}): {hook_name}"
            str_ += "\n" + str(e)
            str_ += "\n" + "\n".join(traceback.format_tb(e.__traceback__))
            if hasattr(e, "characters_written"):
                str_ += "\n" + e.characters_written
        return str_


class DuplicateProcessName(ValueError):
    def __init__(self, process_name):
        self._process_name = process_name

    def __str__(self):
        return (
            f"Duplicate process: {self._process_name}. To include multiple different runs of the same process,"
            f" please include a unique `alias` key for each entry in `spec` of a non-unique `process`"
        )
