from typing import Callable, List, Union

from dataforest.core.TreeSpec import TreeSpec
from dataforest.structures.cache.BranchCache import BranchCache


class TreeProcessMethods:
    def __init__(self, tree_spec: TreeSpec, branch_cache: BranchCache):
        self._tree_spec = tree_spec
        self._branch_cache = branch_cache
        self._process_methods = list()
        self._tether_process_methods()

    @property
    def process_methods(self):
        return self._process_methods

    def _tether_process_methods(self):
        # TODO: docstring
        method_names = [run_group_spec.name for run_group_spec in self._tree_spec]
        for name in method_names:
            distributed_method = self._build_distributed_method(name)
            setattr(self, name, distributed_method)
            self._process_methods.append(distributed_method)

    def _build_distributed_method(self, method_name: str) -> Callable:
        """
        Creates a method which calls the method of `method_name` on all
        branches if that process hasn't already been run for that branch
        Args:
            method_name: name of method corresponding to `alias` key in
                process run spec if it exists, otherwise `process` key

        Returns:
            distributed_method: method which will call `method_name` on all
                branches
        """

        def distributed_method(
            *args,
            stop_on_error: bool = False,
            stop_on_hook_error: bool = False,
            clear_data: Union[bool, List[str]] = True,
            **kwargs,
        ):
            """

            Args:
                *args:
                stop_on_error:
                stop_on_hook_error:
                clear_data: clear cached data attributes of each branch
                    after process execution to save memory. If boolean, whether
                    or not to clear all data attrs, if list, names of data
                    attrs to clear
                **kwargs:

            Returns:

            """
            if not self._branch_cache.fully_loaded:
                self._branch_cache.load_all()
            return_vals = []
            for branch in list(self._branch_cache.values()):
                branch_method = getattr(branch.process, method_name)
                if not branch[method_name].done:
                    return_vals.append(
                        branch_method(
                            *args, stop_on_error=stop_on_error, stop_on_hook_error=stop_on_hook_error, **kwargs
                        )
                    )
                    if clear_data:
                        clear_kwargs = {"all_data": True} if isinstance(clear_data, bool) else {"attrs": clear_data}
                        branch.clear_data(**clear_kwargs)
            return return_vals

        distributed_method.__name__ = method_name
        return distributed_method
