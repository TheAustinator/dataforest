import logging
from pathlib import Path
from typing import Union, Optional, List, Dict

from dataforest.core.DataBase import DataBase
from dataforest.core.DataBranch import DataBranch
from dataforest.core.TreeSpec import TreeSpec
from dataforest.processes.core.TreeProcessMethods import TreeProcessMethods
from dataforest.structures.cache.BranchCache import BranchCache


class DataTree(DataBase):
    _LOG = logging.getLogger("DataTree")
    _BRANCH_CLASS = DataBranch

    def __init__(
        self,
        root: Union[str, Path],
        tree_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        remote_root: Optional[Union[str, Path]] = None,
    ):
        # TODO: add something that tells them how many of each process will be run
        # TODO: add a hook that leaves in "IN_PROGRESS" placeholder file in a run until
        #   it finishes. That way, we can
        super().__init__()
        self.root = root
        self.tree_spec = self._init_spec(tree_spec)
        self._verbose = verbose
        self._current_process = None
        self.remote_root = remote_root
        self._branch_cache = BranchCache(root, self.tree_spec.branch_specs, self._BRANCH_CLASS, verbose, remote_root,)
        self.process = TreeProcessMethods(self.tree_spec, self._branch_cache)

    @property
    def n_branches(self):
        return len(self.tree_spec.branch_specs)

    @property
    def current_process(self):
        return self._current_process if self._current_process else "root"

    def goto_process(self, process_name: str):
        self._LOG.info(f"loading all branches to `goto_process`")
        self._branch_cache.load_all()
        for branch in self._branch_cache.values():
            branch.goto_process(process_name)
        self._current_process = process_name

    def update_process_spec(self, process_name: str, process_spec: dict):
        self.tree_spec[process_name] = process_spec
        self.update_spec(self.tree_spec)

    def update_spec(self, tree_spec: Union[List[dict], "TreeSpec[RunGroupSpec]"]):
        tree_spec = list(tree_spec)
        self.tree_spec = self._init_spec(tree_spec)
        self._branch_cache.update_branch_specs(self.tree_spec.branch_specs)
        self.process = TreeProcessMethods(self.tree_spec, self._branch_cache)

    def run_all(self, workers: int = 1, batch_queue: Optional[str] = None):
        return [method() for method in self.process.process_methods]

    def create_root_plots(self, plot_kwargs: Optional[Dict[str, dict]] = None):
        rand_spec = self.tree_spec.branch_specs[0]
        rand_branch = self._branch_cache[str(rand_spec)]
        rand_branch.create_root_plots(plot_kwargs)

    @staticmethod
    def _init_spec(tree_spec: Union[list, TreeSpec]) -> TreeSpec:
        if tree_spec is None:
            tree_spec = list()
        if not isinstance(tree_spec, TreeSpec):
            tree_spec = TreeSpec(tree_spec)
        return tree_spec
