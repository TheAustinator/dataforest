import logging
from pathlib import Path
from typing import Union, Optional, List, Dict

from dataforest.core.DataBase import DataBase
from dataforest.core.DataBranch import DataBranch
from dataforest.core.PlotTreeMethods import PlotTreeMethods
from dataforest.core.ProcessTreeRun import ProcessTreeRun
from dataforest.core.TreeDataFrame import DataFrameList
from dataforest.core.TreeSpec import TreeSpec
from dataforest.processes.core.TreeProcessMethods import TreeProcessMethods
from dataforest.structures.cache.BranchCache import BranchCache


class DataTree(DataBase):
    _LOG = logging.getLogger("DataTree")
    _BRANCH_CLASS = DataBranch
    _PLOT_METHODS = PlotTreeMethods

    def __init__(
        self,
        root: Union[str, Path],
        tree_spec: Optional[List[dict]] = None,
        twigs: Optional[List[dict]] = None,
        verbose: bool = False,
        remote_root: Optional[Union[str, Path]] = None,
    ):
        # TODO: add something that tells them how many of each process will be run
        # TODO: add a hook that leaves in "IN_PROGRESS" placeholder file in a run until
        #   it finishes. That way, we can
        super().__init__()
        self.root = root
        self.tree_spec = self._init_spec(tree_spec, twigs)
        self._twigs = twigs
        self._verbose = verbose
        self._current_process = None
        self.remote_root = remote_root
        self._branch_cache = BranchCache(root, self.tree_spec.branch_specs, self._BRANCH_CLASS, verbose, remote_root,)
        self._process_tree_runs = dict()
        self.process = TreeProcessMethods(self)

    @property
    def meta(self):
        return DataFrameList([branch.meta for branch in self.branches])

    @property
    def n_branches(self):
        return len(self.tree_spec.branch_specs)

    @property
    def current_process(self):
        return self._current_process if self._current_process else "root"

    @property
    def has_sweeps(self) -> bool:
        """Whether or not any sweeps are in the spec"""
        return bool(set.union(*self.tree_spec.sweep_dict.values()))

    @property
    def has_twigs(self):
        return bool(self._twigs)

    @property
    def branches(self):
        self._branch_cache.load_all()
        return list(self._branch_cache.values())

    def goto_process(self, process_name: str):
        self._branch_cache.load_all()
        for branch in self._branch_cache.values():
            branch.goto_process(process_name)
        self._current_process = process_name

    def load_all(self):
        self._branch_cache.load_all()

    def run_all(self, workers: int = 1, batch_queue: Optional[str] = None):
        return [method() for method in self.process.process_methods]

    def unique_branches_at_process(self, process_name: str) -> Dict[str, "DataBranch"]:
        """
        Gets a subset of branches representing those unique up to the specified
        process. From two branches which only become distinguished after
        `process_name`, just one will be selected.
        """
        return {str(branch.spec[:process_name]): branch for branch in self.branches}

    def create_root_plots(self, plot_kwargs: Optional[Dict[str, dict]] = None):
        rand_spec = self.tree_spec.branch_specs[0]
        rand_branch = self._branch_cache[str(rand_spec)]
        rand_branch._generate_root_plots(plot_kwargs)

    def __getitem__(self, process_name: str) -> ProcessTreeRun:
        if process_name not in self._process_tree_runs:
            process_name = "root" if process_name is None else process_name
            process = self.tree_spec[process_name].process if process_name != "root" else "root"
            self._process_tree_runs[process_name] = ProcessTreeRun(self, process_name, process)
        return self._process_tree_runs[process_name]

    @staticmethod
    def _init_spec(tree_spec: Union[list, TreeSpec], twigs: Optional[List[dict]]) -> TreeSpec:
        if tree_spec is None:
            tree_spec = list()
        if not isinstance(tree_spec, TreeSpec):
            tree_spec = TreeSpec(tree_spec, twigs)
        return tree_spec
