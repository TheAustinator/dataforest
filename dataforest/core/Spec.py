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
            _run_spec_lookup: used for lookup by process_name rather than int
            precursors_lookup:
            process_order:
    """

    def __init__(self, spec: Union[List[dict], "Spec[RunSpec]"]):
        super().__init__([RunSpec(item) for item in spec])
        self._run_spec_lookup: Dict[str, "RunSpec"] = self._build_run_spec_lookup()
        self.precursors_lookup: Dict[str, List[str]] = self._build_process_precursors()
        self.process_order: List[str] = [spec_item.name for spec_item in self]

    def copy(self) -> "Spec":
        return deepcopy(self)

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
            >>>         "params": {
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
            for precursor_name in self.precursors_lookup[process_name]:
                run_spec = self[precursor_name]
                operation = getattr(run_spec, operation_name)
                operation_list.append(operation)
        return operation_list

    def _build_run_spec_lookup(self) -> Dict[str, "RunSpec"]:
        """See class definition"""
        process_lookup = dict()
        for run_spec in self:
            process_name = run_spec.name
            if process_name in process_lookup:
                raise DuplicateProcessName(process_name)
            process_lookup[process_name] = run_spec
        return process_lookup

    def _build_process_precursors(self) -> Dict[str, List[str]]:
        """See class definition"""
        precursors = {}
        current_precursors = []
        for spec_item in self:
            precursors[spec_item.name] = current_precursors.copy()
            current_precursors.append(spec_item.name)
        return precursors

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
        """Order alphabetically for deterministic string representation"""
        return order_dict(self)

    def __str__(self):
        return str(self.ordered())
