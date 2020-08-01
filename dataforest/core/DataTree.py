from pathlib import Path
from typing import Union, Optional, List

from dataforest.core.DataBranch import DataBranch
from dataforest.core.TreeSpec import TreeSpec
from dataforest.processes.core.TreeProcessMethods import TreeProcessMethods
from dataforest.structures.cache.BranchCache import BranchCache


class DataTree:
    BRANCH_CLASS = DataBranch

    def __init__(
        self,
        root: Union[str, Path],
        tree_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        current_process: Optional[str] = None,
        remote_root: Optional[Union[str, Path]] = None,
    ):
        # TODO: add something that tells them how many of each process will be run
        # TODO: add a hook that leaves in "IN_PROGRESS" placeholder file in a run until
        #   it finishes. That way, we can
        self.root = root
        self.tree_spec = self._init_spec(tree_spec)
        self._verbose = verbose
        self._current_process = current_process
        self.remote_root = remote_root
        self._branch_cache = BranchCache(root, self.tree_spec.branch_specs, verbose, current_process, remote_root)
        self.process = TreeProcessMethods(self.tree_spec, self.tree_spec.branch_specs)

    def update_process_spec(self, process_spec: dict):
        raise NotImplementedError()

    def update_spec(self, tree_spec: List[dict]):
        raise NotImplementedError()

    def run_all(self):
        raise NotImplementedError()

    @staticmethod
    def _init_spec(tree_spec: Union[list, TreeSpec]) -> TreeSpec:
        if tree_spec is None:
            tree_spec = list()
        if not isinstance(tree_spec, TreeSpec):
            tree_spec = TreeSpec(tree_spec)
        return tree_spec
