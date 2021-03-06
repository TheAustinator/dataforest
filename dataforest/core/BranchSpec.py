import json
from copy import deepcopy
from typing import Union, List, Dict

from typeguard import typechecked

from dataforest.core.RunGroupSpec import RunGroupSpec
from dataforest.core.RunSpec import RunSpec
from dataforest.utils.exceptions import DuplicateProcessName


class BranchSpec(list):
    """
    Specification of a series of `RunSpec`s to be run sequentially, each of
    which provide descriptors for a process run. If two runs of the same
    process are present, they must be `alias`ed to prevent naming conflicts
    (see example). For details on keys in each encapsulated `dict`, see
    `RunSpec`.
    Args:
        spec: core representation which this class wraps
    Examples:
        >>> # NOTE: conceptual illustration only, not real processes
        >>> branch_spec = [
        >>>     {
        >>>         "_PROCESS_": "reduce",    # dimensionality reduction
        >>>         "_ALIAS_": "linear_dim_reduce",
        >>>         "_PARAMS_": {
        >>>             "algorithm": "pca",
        >>>             "n_pcs": 30
        >>>         }
        >>>     },
        >>>     {
        >>>         "_PROCESS_": "reduce",
        >>>         "_ALIAS_": "nonlinear_dim_reduce",
        >>>         "_PARAMS_": ...
        >>>     },
        >>>     {
        >>>         "_PROCESS_": "dispersity"
        >>>         "_PARAMS_": ...
        >>>     }
        >>> ]
        >>> branch_spec = BranchSpec(branch_spec)
        Attributes:
            _run_spec_lookup: used for `RunSpec` lookup by process_name rather
                than int index
            _precursors_lookup{...}:
                {...}: suffix determines whether "root" and the current_process
                    (key) will be included
                Key: process_name (e.g. "cluster")
                Value: list of precursor processes (e.g. ["root", "normalize",
                    "reduce", "cluster] for {_incl_root_curr}
            process_order:
    """

    _RUN_SPEC_CLASS = RunSpec

    def __init__(self, spec: Union[str, List[dict], "BranchSpec[RunSpec]"]):
        if isinstance(spec, str):
            spec = json.loads(spec)
        if not isinstance(spec, (list, tuple)):
            raise ValueError("spec must be convertible to a list or subclass")
        super().__init__([RunSpec(item) for item in spec])
        self._run_spec_lookup: Dict[str, "RunSpec"] = self._build_run_spec_lookup()
        self._precursors_lookup: Dict[str, List[str]] = self._build_precursors_lookup()
        self._precursors_lookup_incl_curr: Dict[str, List[str]] = self._build_precursors_lookup(incl_current=True)
        self._precursors_lookup_incl_root: Dict[str, List[str]] = self._build_precursors_lookup(incl_root=True)
        self._precursors_lookup_incl_root_curr: Dict[str, List[str]] = self._build_precursors_lookup(
            incl_root=True, incl_current=True
        )
        self.process_order: List[str] = [spec_item.name for spec_item in self]

    @property
    def processes(self):
        return [run_spec["_PROCESS_"] for run_spec in self]

    @property
    def shell_str(self):
        """string version which can be passed via shell and loaded via json"""
        return f"'{str(self)}'"

    def copy(self) -> "BranchSpec":
        return deepcopy(self)

    def get_precursors_lookup(self, incl_current: bool = False, incl_root: bool = False) -> Dict[str, List[str]]:
        """
        Gets a lookup of process_name precursors
        Args:
            incl_current: include key process_name in value list
            incl_root: include root in value list

        Examples:
            >>> precursors_lookup = self.get_precursors_lookup(incl_current=True, incl_root=True)
            >>> precursors_lookup["cluster"]
            >>> ["root", "normalize", "reduce", "cluster"]
        """
        if incl_root and incl_current:
            return self._precursors_lookup_incl_root_curr
        elif incl_root:
            return self._precursors_lookup_incl_root
        elif incl_current:
            return self._precursors_lookup_incl_curr
        else:
            return self._precursors_lookup

    def get_subset_list(self, process_name: str) -> List[dict]:
        """See `_get_data_operation_list`"""
        return self._get_data_operation_list(process_name, "subset")

    def get_filter_list(self, process_name: str) -> List[dict]:
        """See `_get_data_operation_list`"""
        return self._get_data_operation_list(process_name, "filter")

    def get_partition_list(self, process_name: str) -> List[set]:
        """See `_get_data_operation_list`"""
        return self._get_data_operation_list(process_name, "partition")

    def _get_data_operation_list(self, process_name: str, operation_name: str) -> List[Union[dict, set]]:
        """
        Get a list of data operations of a specified type for processes up to
        and including that specified by `process_name`.
        Args:
            process_name: last process for which to retrieve data operations
                (usually the process you're at/running)
            operation_name: from {subset, filter, partition}

        Returns:
            operation_list:

        Examples:
            >>> branch_spec = [
            >>>     {
            >>>         "_PROCESS_": "normalize",
            >>>         "_PARAMS_": {}
            >>>         "_SUBSET_": {
            >>>             "indication": {"disease_1", "disease_3"},
            >>>             "collection_center": "mass_general",
            >>>         },
            >>>         "_FILTER_": {
            >>>             "donor": "D115"
            >>>         }
            >>>     },
            >>>     {
            >>>         "_PROCESS_": "reduce",
            >>>         "_ALIAS_": "linear_dim_reduce",
            >>>         "_SUBSET_": {
            >>>             "indication": "disease_1"
            >>>         }
            >>>     },
            >>>     {
            >>>         "_PROCESS_": "reduce",
            >>>         "_ALIAS_": "nonlinear_dim_reduce",
            >>>         "_PARAMS_": {}
            >>>         "_SUBSET_": {
            >>>             "sample_date": "20200115"
            >>>         }
            >>>     }
            >>> ]
            >>> branch_spec = BranchSpec(branch_spec)
            >>> subset_list = branch_spec._get_data_operation_list("linear_dim_reduce", "subset")
            >>> subset_list
            >>> [
            >>>     {
            >>>         "indication": {"disease_1", "disease_3"},
            >>>         "collection_center": "mass_general",
            >>>     },
            >>>     {
            >>>         "indication": "disease_1"
            >>>     }
            >>> ]
        """
        operation_list = []
        if process_name:
            process_name_list = self.get_precursors_lookup(incl_current=True)[process_name]
            for precursor_name in process_name_list:
                run_spec = self[precursor_name]
                operation = getattr(run_spec, operation_name)
                operation_list.append(operation)
        return operation_list

    def _build_run_spec_lookup(self) -> Dict[str, Union["RunSpec", "RunGroupSpec"]]:
        """See class definition"""
        run_spec_lookup = {"root": self._RUN_SPEC_CLASS({})}
        for run_spec in self:
            process_name = run_spec.name
            if process_name in run_spec_lookup:
                raise DuplicateProcessName(process_name)
            run_spec_lookup[process_name] = run_spec
        return run_spec_lookup

    def _build_precursors_lookup(self, incl_root: bool = False, incl_current: bool = False) -> Dict[str, List[str]]:
        """See class definition"""
        current_precursors = []
        if incl_root and incl_current:
            current_precursors = current_precursors + ["root"]
        precursors = {"root": current_precursors}
        if incl_root and not incl_current:
            current_precursors = current_precursors + ["root"]
        for spec_item in self:
            if incl_current:
                current_precursors = current_precursors + [spec_item.name]
            precursors[spec_item.name] = current_precursors
            if not incl_current:
                current_precursors = current_precursors + [spec_item.name]
        return precursors

    @typechecked
    def __getitem__(self, key: Union[str, int, slice]) -> Union["RunSpec", "BranchSpec"]:
        """Get `RunSpec` either via `int` index or `name`"""
        if isinstance(key, str):
            try:
                return self._run_spec_lookup[key]
            except KeyError:
                raise KeyError(
                    f"'{key}' not found in spec. Ensure that you've passed `branch_spec` or `tree_spec` to `cf.load`, "
                    f"`cf.from_metadata`, or whichever init function you used. Current spec: {self}"
                )
        elif isinstance(key, slice):
            if isinstance(key.stop, str):
                if key.start or key.step:
                    raise ValueError(f"Can only use stop with string slice (ex. [:'process_name'])")
                precursors_lookup = self.get_precursors_lookup(incl_current=True)
                precursors = precursors_lookup[key.stop]
                return self.__class__([self._run_spec_lookup[process] for process in precursors])
            else:
                return self.__class__(super().__getitem__(key))
        else:
            return super().__getitem__(key)

    def __setitem__(self, k, v):
        raise NotImplementedError("Cannot set items dynamically. All items must be defined at init")

    def __contains__(self, item):
        return item in self._run_spec_lookup

    def __str__(self):
        return super().__str__().replace("'", '"')
