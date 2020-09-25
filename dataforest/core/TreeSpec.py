from itertools import product
from typing import Union, List

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

    def __init__(self, tree_spec: Union[List[dict], "TreeSpec[RunGroupSpec]"]):
        super(list, self).__init__()
        self.extend([RunGroupSpec(item) for item in tree_spec])
        self.branch_specs = self._build_branch_specs()
        self.sweep_dict = {x["_PROCESS_"]: x.sweeps for x in self}
        self._raw = tree_spec

    def _build_branch_specs(self):
        return list(map(BranchSpec, product(*[run_group_spec.run_specs for run_group_spec in self])))

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            idx_lookup = {run_group_spec.name: i for i, run_group_spec in enumerate(self)}
            key = idx_lookup[key]
        list.__setitem__(self, key, value)


class PlotSpec:
    def __init__(self, tree, plot_key, use_saved=True, **kwargs):
        self._tree = tree
        self._branch_spec = tree.tree_spec.branch_specs[0]
        self._plot_key = plot_key
        self._use_saved = use_saved
        self._kwargs = kwargs
        self._sweeps_remaining = len(self._tree.tree_spec.sweep_dict[self._tree.current_process])

    def update(self, process, param, value):
        self._branch_spec[process]["_PARAMS_"][param] = value

    def get_updater(self, process, param):
        def updater(value):
            self._branch_spec[process]["_PARAMS_"][param] = value
            branch = self._tree._branch_cache[str(self)]
            plot_map = branch[process].plot_map
            plot_path_lookup = {plot_key: next(iter(path_dict.values())) for plot_key, path_dict in plot_map.items()}
            if self._plot_key in plot_path_lookup and self._use_saved:
                plot_path = plot_path_lookup.get(self._plot_key)
                if plot_path.exists():
                    return Image(plot_path)
            self._generate_plot(branch, process)

        return updater

    def _generate_plot(self, branch: "DataBranch", process: str):
        method = branch.plot.method_lookup[self._plot_key]
        # fig, ax = method(**self._kwargs)
        method(**self._kwargs)
        # return fig, ax

    def __str__(self):
        return str(self._branch_spec)

    def __repr__(self):
        return repr(self._branch_spec)
