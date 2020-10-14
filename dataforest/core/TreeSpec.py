from itertools import product
from typing import Union, List, Dict, Any

from dataforest.core.BranchSpec import BranchSpec
from dataforest.core.RunGroupSpec import RunGroupSpec


class TreeSpec(BranchSpec):
    """
    >>> tree_spec = [
    >>>     {
    >>>         "_PROCESS_": "normalize",
    >>>         "_PARAMS_": [
    >>>             {
    >>>                 "min_genes": 5,
    >>>                 "max_genes": {"_SWEEP_": {"min": 2000, "max": 8000, "step": 1000}},
    >>>                 "min_cells": 5,
    >>>                 "nfeatures": {"_SWEEP_": {"min": 2, "max": 8, "base": 2}},
    >>>                 "perc_mito_cutoff": 20,
    >>>                 "method": "seurat_default",
    >>>             },
    >>>             {
    >>>                 "min_genes": 5,
    >>>                 "max_genes": 5000,
    >>>                 "min_cells": 5,
    >>>                 "perc_mito_cutoff": 20,
    >>>                 "method": "sctransform"
    >>>             },
    >>>         ],
    >>>         "_SUBSET_": {
    >>>             "sample_id": {"_SWEEP_": ["sample_1", "sample_2"]}
    >>>         },
    >>>     },
    >>>     {
    >>>         "_PROCESS_": "reduce",
    >>>         "_PARAMS_": {
    >>>             "pca_npcs": {"_SWEEP_": {"min": 2, "max": 5, "base": 2}},
    >>>             "umap_n_neighbors": {"_SWEEP_": {"min": 2, "max": 5, "step": 1}},
    >>>             "umap_min_dist": 0.1,
    >>>             "umap_n_components": 2,
    >>>             "umap_metric": {"_SWEEP_": ["cosine", "euclidean"]},
    >>>         },
    >>>     },
    >>> ]
    >>> tree_spec = TreeSpec(tree_spec)
    """

    _RUN_SPEC_CLASS = RunGroupSpec

    def __init__(self, tree_spec: Union[List[dict], "TreeSpec[RunGroupSpec]"]):
        super(list, self).__init__()
        self.extend([RunGroupSpec(item) for item in tree_spec])
        self.branch_specs = self._build_branch_specs()
        self.sweep_dict = {x["_PROCESS_"]: x.sweeps for x in self}
        self.sweep_dict["root"] = set()
        self._run_spec_lookup: Dict[str, "RunGroupSpec"] = self._build_run_spec_lookup()
        self._raw = tree_spec
        self._run_spec_lookup: Dict[str, "RunSpec"] = self._build_run_spec_lookup()
        self._precursors_lookup: Dict[str, List[str]] = self._build_precursors_lookup()
        self._precursors_lookup_incl_curr: Dict[str, List[str]] = self._build_precursors_lookup(incl_current=True)
        self._precursors_lookup_incl_root: Dict[str, List[str]] = self._build_precursors_lookup(incl_root=True)
        self._precursors_lookup_incl_root_curr: Dict[str, List[str]] = self._build_precursors_lookup(
            incl_root=True, incl_current=True
        )

    def _build_branch_specs(self):
        return list(map(BranchSpec, product(*[run_group_spec.run_specs for run_group_spec in self])))

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            idx_lookup = {run_group_spec.name: i for i, run_group_spec in enumerate(self)}
            key = idx_lookup[key]
        list.__setitem__(self, key, value)
