import logging
from pathlib import Path
from typing import List, Union

from dataforest.core.BranchSpec import BranchSpec
from dataforest.core.DataBranch import DataBranch
from dataforest.structures.cache.HashCash import HashCash


class BranchCache(HashCash):
    _LOG = logging.getLogger("BranchCache")

    def __init__(self, root: Union[str, Path], branch_specs: List[BranchSpec], branch_class: type, *args):
        super().__init__()
        self._root = root
        self._branch_spec_lookup = {str(spec): spec for spec in branch_specs}
        self._branch_class = branch_class
        self._branch_args = args
        self.fully_loaded = False

    def load_all(self):
        self._LOG.info(f"loading all branches to `goto_process`")
        if not self.fully_loaded:
            for spec_str in self._branch_spec_lookup:
                _ = self[spec_str]  # force load of all items
            self.fully_loaded = True

    def update_branch_specs(self, branch_specs: List[BranchSpec]):
        self._branch_spec_lookup = {str(spec): spec for spec in branch_specs}
        self.fully_loaded = False

    def _get(self, branch_spec: Union[str, BranchSpec]) -> "DataBranch":
        spec_str = str(branch_spec)
        branch_spec = self._branch_spec_lookup[spec_str]
        branch = self._branch_class(self._root, branch_spec, *self._branch_args)
        return branch
