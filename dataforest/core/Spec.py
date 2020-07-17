import json
from copy import deepcopy
from typing import Union, List

from dataforest.utils.exceptions import DuplicateProcessName
from dataforest.utils.utils import order_dict


class Spec(list):
    def __init__(self, spec):
        super().__init__([RunSpec(item) for item in spec])
        self.process_lookup = self._build_process_lookup()
        self.precursors_lookup = self._build_process_precursors()
        self.process_order = [spec_item.name for spec_item in self]

    def copy(self) -> "Spec":
        return deepcopy(self)

    def get_subset_list(self, process_name: str) -> List[dict]:
        return self._get_data_operation_list(process_name, "subset")

    def get_filter_list(self, process_name: str) -> List[dict]:
        return self._get_data_operation_list(process_name, "filter")

    def get_partition_list(self, process_name: str) -> List[set]:
        return self._get_data_operation_list(process_name, "partition")

    def _get_data_operation_list(self, process_name: str, operation_name: str) -> List[Union[dict, set]]:
        operation_list = []
        if process_name:
            for precursor_name in self.precursors_lookup[process_name]:
                run_spec = self[precursor_name]
                operation = getattr(run_spec, operation_name)
                operation_list.append(operation)
        return operation_list

    def _build_process_lookup(self):
        process_lookup = dict()
        for run_spec in self:
            process_name = run_spec.name
            if process_name in process_lookup:
                raise DuplicateProcessName(process_name)
            process_lookup[process_name] = run_spec
        return process_lookup

    def _build_process_precursors(self):
        precursors = {}
        current_precursors = []
        for spec_item in self:
            precursors[spec_item.name] = current_precursors.copy()
            current_precursors.append(spec_item.name)
        return precursors

    def __getitem__(self, item: Union[str, int]) -> "RunSpec":
        if not isinstance(item, int):
            return self.process_lookup[item]
        else:
            return super().__getitem__(item)

    def __setitem__(self, k, v):
        raise ValueError("Cannot set items dynamically. All items must be defined at init")

    def __contains__(self, item):
        return item in self.process_lookup


class RunSpec(dict):
    @property
    def name(self):
        return self.get("alias", self["process"])

    @property
    def process(self):
        return self["process"]

    @property
    def alias(self):
        return self.get("alias", None)

    @property
    def params(self):
        return self.get("params", {})

    @property
    def subset(self):
        return self.get("subset", {})

    @property
    def filter(self):
        return self.get("filter", {})

    @property
    def partition(self):
        return self.get("partition", {})

    def ordered(self):
        return order_dict(self)

    def __str__(self):
        return str(self.ordered())
