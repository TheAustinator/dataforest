import json
from copy import deepcopy
from typing import Union, List, Dict, Optional

from dataforest.utils.exceptions import DuplicateProcessName
from dataforest.utils.utils import order_dict


class Spec(list):
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
        >>> spec = [
        >>>     {
        >>>         "process": "normalize",
        >>>         "params": {
        >>>             "min_genes": 5,
        >>>             "max_genes": 5000,
        >>>             "min_cells": 5,
        >>>             "nfeatures": 30,
        >>>             "perc_mito_cutoff": 20,
        >>>             "method": "seurat_default",
        >>>         }
        >>>         "subset": {
        >>>             "indication": {"disease_1", "disease_3"},
        >>>             "collection_center": "mass_general",
        >>>         },
        >>>         "filter": {
        >>>             "donor": "D115"
        >>>         }
        >>>     },
        >>>     {
        >>>         "process": "reduce",    # dimensionality reduction
        >>>         "alias": "linear_dim_reduce",
        >>>         "params": {
        >>>             "algorithm": "pca",
        >>>             "n_pcs": 30,
        >>>         }
        >>>     },
        >>>     {
        >>>         "process": "reduce",
        >>>         "alias": "nonlinear_dim_reduce",
        >>>         "params": {
        >>>             "algorithm": "umap",
        >>>             "n_neighbors": 15,
        >>>             "min_dist": 0.1,
        >>>             "n_components": 2,
        >>>             "metric": "euclidean"
        >>>         }
        >>>     }
        >>> ]
        >>> spec = Spec(spec)
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

    def __init__(self, spec: Union[List[dict], "Spec[RunSpec]"]):
        super().__init__([RunSpec(item) for item in spec])
        self._run_spec_lookup: Dict[str, "RunSpec"] = self._build_run_spec_lookup()
        self._precursors_lookup: Dict[str, List[str]] = self._build_precursors_lookup()
        self._precursors_lookup_incl_curr: Dict[str, List[str]] = self._build_precursors_lookup_incl_curr()
        self._precursors_lookup_incl_root: Dict[str, List[str]] = self._build_precursors_lookup_incl_root()
        self._precursors_lookup_incl_root_curr: Dict[str, List[str]] = self._build_precursors_lookup_incl_root_curr()
        self.process_order: List[str] = [spec_item.name for spec_item in self]

    def copy(self) -> "Spec":
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
            >>> spec = [
            >>>     {
            >>>         "process": "normalize",
            >>>         "params": {}
            >>>         "subset": {
            >>>             "indication": {"disease_1", "disease_3"},
            >>>             "collection_center": "mass_general",
            >>>         },
            >>>         "filter": {
            >>>             "donor": "D115"
            >>>         }
            >>>     },
            >>>     {
            >>>         "process": "reduce",
            >>>         "alias": "linear_dim_reduce",
            >>>         "subset": {
            >>>             "indication": "disease_1"
            >>>         }
            >>>     },
            >>>     {
            >>>         "process": "reduce",
            >>>         "alias": "nonlinear_dim_reduce",
            >>>         "params": {}
            >>>         "subset": {
            >>>             "sample_date": "20200115"
            >>>         }
            >>>     }
            >>> ]
            >>> spec = Spec(spec)
            >>> subset_list = spec._get_data_operation_list("linear_dim_reduce", "subset")
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

    def _build_run_spec_lookup(self) -> Dict[str, "RunSpec"]:
        """See class definition"""
        run_spec_lookup = {"root": RunSpec({})}
        for run_spec in self:
            process_name = run_spec.name
            if process_name in run_spec_lookup:
                raise DuplicateProcessName(process_name)
            run_spec_lookup[process_name] = run_spec
        return run_spec_lookup

    def _build_precursors_lookup(self) -> Dict[str, List[str]]:
        """See class definition"""
        precursors = {"root": []}
        current_precursors = []
        for spec_item in self:
            precursors[spec_item.name] = current_precursors.copy()
            current_precursors.append(spec_item.name)
        return precursors

    def _build_precursors_lookup_incl_curr(self) -> Dict[str, List[str]]:
        """See class definition"""
        precursor_lookup = deepcopy(self._precursors_lookup)
        for process_name, precursors in precursor_lookup.items():
            if process_name not in precursors:
                precursors.append(process_name)
        return precursor_lookup

    def _build_precursors_lookup_incl_root(self):
        """See class definition"""
        precursor_lookup = deepcopy(self._precursors_lookup)
        for process_name, precursors in precursor_lookup.items():
            if "root" not in precursors:
                precursors.insert(0, "root")
        return precursor_lookup

    def _build_precursors_lookup_incl_root_curr(self):
        """See class definition"""
        precursor_lookup = deepcopy(self._precursors_lookup)
        for process_name, precursors in precursor_lookup.items():
            if "root" not in precursors:
                precursors.insert(0, "root")
            if process_name not in precursors:
                precursors.append(process_name)
        return precursor_lookup

    def __getitem__(self, item: Union[str, int]) -> "RunSpec":
        """Get `RunSpec` either via `int` index or `name`"""
        if not isinstance(item, int):
            return self._run_spec_lookup[item]
        else:
            return super().__getitem__(item)

    def __setitem__(self, k, v):
        raise ValueError("Cannot set items dynamically. All items must be defined at init")

    def __contains__(self, item):
        return item in self._run_spec_lookup


class RunSpec(dict):
    """
    Specification to run a single process.
    Keys:
        process (str): name of the `dataprocess` decorated function to execute
        alias (Optional[str]): given name, which is required when multiple of
            the same `process` are to exist in the same `Spec`
        params: parameters for `process`
        subset, filter, partition: see dataforest.core.DataForest docs
    """

    @property
    def name(self) -> str:
        return self.get("alias", self["process"])

    @property
    def process(self) -> str:
        return self["process"]

    @property
    def alias(self) -> Optional[str]:
        return self.get("alias", None)

    @property
    def params(self) -> dict:
        return self.get("params", {})

    @property
    def subset(self) -> dict:
        return self.get("subset", {})

    @property
    def filter(self) -> dict:
        return self.get("filter", {})

    @property
    def partition(self) -> dict:
        return self.get("partition", {})

    def ordered(self) -> dict:
        """Order dict alphabetically for deterministic string representation"""
        return order_dict(self)

    def __str__(self):
        return str(self.ordered())
