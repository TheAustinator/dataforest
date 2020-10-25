import logging
from typing import Callable, List, Union, TYPE_CHECKING

from joblib import Parallel, delayed

from dataforest.core.TreeSpec import TreeSpec
from dataforest.structures.cache.BranchCache import BranchCache

if TYPE_CHECKING:
    from dataforest.core.DataTree import DataTree


class TreeProcessMethods:
    _N_CPUS_EXCLUDED = 1

    def __init__(self, tree: "DataTree"):
        self._tree = tree
        self._process_methods = list()
        self._tether_process_methods()

    @property
    def process_methods(self):
        return self._process_methods

    def _tether_process_methods(self):
        # TODO: docstring
        method_names = [run_group_spec.name for run_group_spec in self._tree.tree_spec]
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

        def _distributed_method(
            *args,
            stop_on_error: bool = False,
            stop_on_hook_error: bool = False,
            clear_data: Union[bool, List[str]] = True,
            force_rerun: bool = False,
            parallel: bool = False,
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
                force_rerun: force the process to rerun even if already done
                parallel: whether or not to parallelize
                **kwargs:

            Returns:

            """
            kwargs = {"stop_on_error": stop_on_error, "stop_on_hook_error": stop_on_hook_error, **kwargs}
            unique_branches = self._tree.unique_branches_at_process(method_name)

            def _single_kernel(branch):
                branch_method = getattr(branch.process, method_name)
                if not branch[method_name].done or force_rerun:
                    ret = branch_method(*args, **kwargs)
                    if clear_data:
                        clear_kwargs = {"all_data": True} if isinstance(clear_data, bool) else {"attrs": clear_data}
                        branch.clear_data(**clear_kwargs)
                    return ret

            def _distributed_kernel_serial():
                _ret_vals = []
                for branch in unique_branches.values():
                    _ret_vals.append(_single_kernel(branch))
                return _ret_vals

            def _distributed_kernel_parallel():
                process = delayed(_single_kernel)
                pool = Parallel(n_jobs=-1 - self._N_CPUS_EXCLUDED)
                return pool(process(branch) for branch in unique_branches.values())

            exec_scheme = "PARALLEL" if parallel else "SERIAL"
            logging.info(f"{exec_scheme} execution of {method_name} on {len(unique_branches)} unique branches")
            kernel = _distributed_kernel_parallel if parallel else _distributed_kernel_serial
            ret_vals = kernel()
            return ret_vals

        _distributed_method.__name__ = method_name
        return _distributed_method

    def _distributed_kernel_parallel(self):
        raise NotImplementedError()
