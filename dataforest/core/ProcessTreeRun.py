import logging
from typing import TYPE_CHECKING, Callable, Set, Tuple

from dataforest.core.DataBranch import DataBranch

if TYPE_CHECKING:
    from dataforest.core.DataTree import DataTree


class ProcessTreeRun:
    def __init__(self, tree: "DataTree", process_name: str, process: str):
        self._LOG = logging.getLogger(f"ProcessRun - {process_name}")
        if process_name not in tree.tree_spec and process_name != "root":
            raise ValueError(f'key "{process_name}" not in tree_spec: {tree.tree_spec}')
        self.process_name = process_name
        self.process = process
        self._tree = tree

    @property
    def done(self):
        self._tree.load_all()
        return all(map(lambda branch: branch[self.process_name].done, self._tree._branch_cache.values()))

    @property
    def failed(self):
        filter_ = lambda branch, process: branch[process].done and not branch[process].success
        return self._filter_branches(filter_)

    @property
    def success(self):
        filter_ = lambda branch, process: branch[process].done and branch[process].success
        return self._filter_branches(filter_)

    def _filter_branches(self, filter_: Callable) -> tuple:
        self._tree.load_all()
        branches = self._tree._branch_cache.values()
        process = self.process_name
        return tuple([branch for branch in branches if filter_(branch, process)])
