from typing import List, Callable, Dict, Any, TYPE_CHECKING, Optional

from dataforest.hyperparams.Sweep import Sweep

if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch


class HyperparameterMethods:
    def __init__(self, branch: "DataBranch"):
        self.branch = branch

    def sweep(
        self,
        sweep_dict: Dict[str, Dict[str, Any]],
        methods: List[Callable],
        method_kwargs: Optional[List[dict]] = None,
        skip_if_done: Optional[str] = None,
        stop_on_error: bool = False,
        combinatorial: bool = False,
    ):
        """Runs a hyperparameter sweep. See `Sweep` documentation."""
        sweep = Sweep(self.branch, sweep_dict, combinatorial)
        sweep.run(methods, method_kwargs, skip_if_done, stop_on_error)
        return sweep
