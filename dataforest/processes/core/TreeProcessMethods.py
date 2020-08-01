from typing import Callable

from dataforest.core.TreeSpec import TreeSpec
from dataforest.structures.cache.BranchCache import BranchCache


class TreeProcessMethods:
    def __init__(self, tree_spec: TreeSpec, branch_cache: BranchCache):
        self._tree_spec = tree_spec
        self._branch_cache = branch_cache
        self._tether_process_methods()

    def _tether_process_methods(self):
        method_names = [run_group_spec.name for run_group_spec in self._tree_spec]
        for name in method_names:
            setattr(self, name, self._get_distributed_method(name))

    def _get_distributed_method(self, method_name: str) -> Callable:
        def distributed_method():
            if not self._branch_cache.fully_loaded:
                self._branch_cache.load_all()
            branches = list(self._branch_cache.values())
            return [getattr(branch.process, method_name)() for branch in branches]

        distributed_method.__name__ = method_name
        return distributed_method
