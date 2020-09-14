from dataforest.core.BranchSpec import BranchSpec
from dataforest.config.MetaProcessMethods import MetaProcessMethods
from dataforest.utils import copy_func, tether


class ProcessMethods(metaclass=MetaProcessMethods):
    # TODO: docstring
    """
    Container base class for `staticmethod`s which execute `processes` in a
    processes system defined by a `ProcessSchema`. Methods should be decorated
    with the `dataprocess` hook to specify their upstream processes dependencies
    so that the correct input data can be located and validated. Methods are
    also tethered to the class attached DataBranch if the instantiated

    Method names must match `ProcessSchema`
    """

    def __init__(self, branch: "DataBranch", spec: BranchSpec):
        self.branch = branch
        alias_dict = {run_spec.name: run_spec.process for run_spec in spec}
        callable_lookup = self.__class__.PROCESSES
        for name, process_name in alias_dict.items():
            callable_ = copy_func(callable_lookup[process_name])
            callable_.__name__ = name
            setattr(self, name, callable_)
        tether(self, "branch")
