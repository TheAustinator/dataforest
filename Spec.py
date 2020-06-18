import json
from copy import deepcopy
from pathlib import Path
from pprint import pprint
from typing import Set, Union, Optional

from dataforest.DataTree import DataTree
from dataforest.ProcessSchema import ProcessSchema
from dataforest.Tree import Tree
from dataforest.utils import update_recursive


class Spec(dict):
    """
    Base class for specification of path through `process_run` for `ORM` to
    interface with data. Going deeper into the tree does not limit
    the `ORM`s ability to interface with data at earlier nodes so long as they
    are along the same path.
    Examples:
        {
          "process_1": {
            "param_1": 10,
            "param_2": "max",
            "version": (1.05, 1.06, 1.07, 1.08, 1.09),
          "process_2": {
            "alpha": 0.05,
            "thresh": 2.0,
            "filter": {"version": 1.07},
        "filter": {"version": 1.07},
        }

        In this example, `process_1` is fed two parameters, and also "version"
        as a `subset`. So the data that is fed to `process_2` is also `subset`
        by "version". However, `process_2` further excluded `version=1.07`
        using a `filter`. 1.07 will be excluded from the outputs of `process_2`
        and any downstream processes, but it won't be excluded from any
        upstream data that we want to combine with the `process_2` outputs,
        so the `filter` is also specified at the root level so that it can
        be excluded either manually or in any data access methods of `ORM`
        subclasses.
    """

    DEFAULT_SPEC = {}

    def __init__(self, data_map, schema: ProcessSchema, spec_dict=None):
        super().__init__()
        self.update(deepcopy(self.DEFAULT_SPEC))
        # self.get_matching_branches(data_map, schema, spec_dict)
        # spec_dict = Tree(spec_dict).str_replace_leaves(" ", "_").dict
        update_recursive(self, spec_dict, inplace=True)
        # TODO: these won't be updated dynamically
        self._process_spec_dict = self.get_process_spec_dict(self, schema)
        self._subset_dict = self.get_subset_dict(self, schema)
        self._filter_dict = self.get("filter", dict())
        self._partition_set = self.get("partition", dict())

    @property
    def process_spec_dict(self) -> dict:
        return self._process_spec_dict

    @property
    def subset_dict(self) -> dict:
        return self._subset_dict

    @property
    def filter_dict(self) -> dict:
        return self._filter_dict

    @property
    def partition_set(self) -> Set[str]:
        return self._partition_set

    @classmethod
    def from_str(cls, path: Union[str, Path], data_dir: Union[str, Path]):
        spec = dict()
        rel_path = Path(path).relative_to(data_dir)
        parts = rel_path.parts
        for (process_name, process_str) in zip(parts[::2], parts[1::2]):
            spec[process_name] = DataTree.from_str(process_str, []).simple
        return spec

    def get_matching_branches(self, data_map, schema, spec_dict):
        # TODO: get working
        precursor_dict = schema.process_precursors
        process_spec_dict = {k: v for k, v in spec_dict.items() if k in precursor_dict}
        process_spec_tree = Tree(process_spec_dict).apply_leaves(str)  # convert keys and values to strings
        spec_depth = max([len(precursor_dict[name]) + 1 for name in process_spec_dict] + [0])
        if spec_depth == 0:
            return
        isodepth_branches = [branch for branch in data_map if len(branch) == spec_depth]
        matches = [branch for branch in isodepth_branches if process_spec_tree.issubset(branch)]
        match = None
        if not matches:
            pprint(isodepth_branches)
            raise ValueError(f"No branches matching spec. Available branches of the same depth: {isodepth_branches}")
        elif len(matches) == 1:
            match = matches[0]
        else:
            for candidate in matches:
                if candidate == process_spec_dict:
                    match = candidate
        if match is None:
            variable_paths = Tree(matches[0]).variable_paths(matches[1:])
            pprint(isodepth_branches)
            raise ValueError(f"Underspecified. Must discriminate between {variable_paths}")
        self.update(match)

    def print(self):
        def set_to_list(x):
            if isinstance(x, set):
                return list(x)
            return x

        dict_ = Tree(self).apply_leaves(set_to_list).dict
        print(json.dumps(dict_, indent=4))

    def copy(self) -> "Spec":
        return deepcopy(self)

    @staticmethod
    def get_process_spec_dict(dict_: Union[dict, "Spec"], schema: ProcessSchema):
        return {k: v for k, v in dict_.items() if k in schema.PROCESS_NAMES}

    @staticmethod
    def get_subset_dict(dict_: Union[dict, "Spec"], schema: ProcessSchema, process_name: Optional[str] = None):
        if process_name:
            keys_exclude = schema.param_names[process_name]
            dict_ = dict_[process_name]
        else:
            keys_exclude = Spec.get_process_spec_dict(dict_, schema)
        return {k: v for k, v in dict_.items() if (k not in ("filter", "partition")) and (k not in keys_exclude)}

    @staticmethod
    def get_filter_dict(dict_: Union[dict, "Spec"], process_name: Optional[str] = None):
        if process_name:
            return dict_[process_name].get("filter", dict())
        else:
            return dict_.get("filter", dict())
