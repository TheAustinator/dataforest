from copy import deepcopy
from itertools import product
from typing import Union, List, Dict, TYPE_CHECKING, Optional

from dataforest.core.BranchSpec import BranchSpec
from dataforest.core.RunGroupSpec import RunGroupSpec

if TYPE_CHECKING:
    from dataforest.core.RunSpec import RunSpec


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

    def __init__(self, tree_spec: Union[List[dict], "TreeSpec[RunGroupSpec]"], twigs: Optional[List[dict]]):
        super(list, self).__init__()
        self.extend([RunGroupSpec(item) for item in tree_spec])
        self.twig_specs = dict()
        self.branch_specs = self._build_branch_specs(twigs)
        self.sweep_dict = {x["_PROCESS_"]: x.sweeps for x in self}
        self.sweep_dict["root"] = set()
        self._raw = tree_spec
        self._run_spec_lookup: Dict[str, "RunGroupSpec"] = self._build_run_spec_lookup()
        self._precursors_lookup: Dict[str, List[str]] = self._build_precursors_lookup()
        self._precursors_lookup_incl_curr: Dict[str, List[str]] = self._build_precursors_lookup(incl_current=True)
        self._precursors_lookup_incl_root: Dict[str, List[str]] = self._build_precursors_lookup(incl_root=True)
        self._precursors_lookup_incl_root_curr: Dict[str, List[str]] = self._build_precursors_lookup(
            incl_root=True, incl_current=True
        )

    def _build_branch_specs(self, twigs: Optional[List[dict]]):
        specs = list(map(BranchSpec, product(*[run_group_spec.run_specs for run_group_spec in self])))
        template = specs[0]
        specs.extend(self._add_twigs(template, twigs))
        return specs

    def _add_twigs(self, template: "RunSpec", twigs: Optional[List[Union[tuple, list]]]):
        specs = list()
        self.twig_specs["base"] = template
        if twigs is None:
            return specs
        for twig in twigs:
            spec_ = deepcopy(template)
            if isinstance(twig, tuple):
                twig = [twig]
            for mod in twig:
                val = mod[-1]
                final_key = mod[-2]
                accessors = mod[:-2]
                scope = spec_
                for key in accessors:
                    scope = scope[key]
                scope[final_key] = val
            specs.append(spec_)
            self.twig_specs[str(twig)] = spec_
        return specs

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            idx_lookup = {run_group_spec.name: i for i, run_group_spec in enumerate(self)}
            key = idx_lookup[key]
        list.__setitem__(self, key, value)
