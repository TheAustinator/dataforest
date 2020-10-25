from itertools import product
from typing import Union, Iterable, List, Tuple, Any

from dataforest.core.RunSpec import RunSpec
from dataforest.core.Sweep import Sweep
from dataforest.utils.utils import order_dict


class RunGroupSpec(RunSpec):
    """
    # TODO: show list operation values
    # TODO: show sweeps
    """

    # could also name ProcessLayerSpec
    # TODO: make SpecDictBase for shared components of this and RunSpec

    def __init__(self, dict_: Union[dict, "RunGroupSpec"]):
        super().__init__(**dict_)
        self.sweeps = set()
        self.run_specs = self._build_run_specs()

    def _build_run_specs(self):
        """
        Three step recursive process to expand all sweeps in input into a
        combinatorial list of `RunSpec`s representing all combinations
        represented by all sweeps combined.
        1. Any list operation values at the base level of dict are expanded
        2. Any _SWEEP_ keys are converted to `Sweep`s and expanded back to
            lists at base level
        3. Step 1 is repeated due to new base level list values
        Returns:
            run_specs: list of `RunSpec`s representing all combinations of
                values from sweeps
        """
        sub_groups, combos = self._expand_sweeps(self, (list, set, tuple))
        # if there were no operations in array format to be expanded
        if isinstance(sub_groups, dict):
            self._map_sweeps()
            # expand _SWEEP_ keys
            self_expand = {k: self._expand_sweeps(v, Sweep)[0] for k, v in self.items() if isinstance(v, dict)}
            self_expand.update({k: v for k, v in self.items() if not isinstance(v, dict)})
            # if any operations are still in array format
            if any(map(lambda x: isinstance(x, list), self_expand.values())):
                self_expand = RunGroupSpec(self_expand)
                run_specs = self_expand.run_specs
            # all operations expanded
            else:
                run_specs = [RunSpec(self)]
        # array format operations were expanded
        else:
            sub_groups = [RunGroupSpec(sub_group) for sub_group in sub_groups]
            run_specs = [run_spec for group in sub_groups for run_spec in group.run_specs]
        return run_specs

    def _map_sweeps(self):
        """Convert "_SWEEP_" keys to `Sweep` objects"""
        for operation, operation_dict in self.items():
            if isinstance(operation_dict, dict):
                for key, val in operation_dict.items():
                    if isinstance(val, dict) and "_SWEEP_" in val:
                        sweep_info = (self["_PROCESS_"], operation, key, tuple(val["_SWEEP_"]))
                        self.sweeps.add(sweep_info)
                        sweep_obj = val["_SWEEP_"]
                        self[operation][key] = Sweep(operation, key, sweep_obj)

    @staticmethod
    # TODO: type hint combos
    def _expand_sweeps(dict_: dict, types: Union[type, Iterable[type]]) -> Tuple[Union[dict, List[dict]], Any]:
        """
        Expand sweeps into a list of dicts representing all possible combinations.
        Args:
            dict_: dict whose values to scan for sweeps
            types: types of values which are considered to be sweeps
        """
        sweeps_part = order_dict({k: v for k, v in dict_.items() if isinstance(v, types)})
        static_part = {k: v for k, v in dict_.items() if k not in sweeps_part}
        combos = list(product(*sweeps_part.values()))
        dicts = [{**static_part, **dict(zip(sweeps_part.keys(), combo))} for combo in combos]
        if len(dicts) == 1:
            dicts = dicts[0]
            combos = combos[0]
        return dicts, combos
