from pathlib import Path

import pytest

from dataforest.core.DataTree import DataTree
from dataforest.core.RunGroupSpec import RunGroupSpec
from dataforest.core.TreeSpec import TreeSpec


@pytest.fixture
def data_dir():
    path = Path(__file__).parent / "data"
    return path


@pytest.fixture
def tree_spec():
    spec = [
        {
            "_PROCESS_": "normalize",
            "_PARAMS_": [
                {
                    "min_genes": 5,
                    "max_genes": {"_SWEEP_": {"min": 2000, "max": 8000, "step": 1000}},
                    "min_cells": 5,
                    "nfeatures": {"_SWEEP_": {"min": 2, "max": 8, "base": 2}},
                    "perc_mito_cutoff": 20,
                    "method": "seurat_default",
                },
                {"min_genes": 5, "max_genes": 5000, "min_cells": 5, "perc_mito_cutoff": 20, "method": "sctransform"},
            ],
            "_SUBSET_": {"sample": {"_SWEEP_": ["sample_1", "sample_2"]}},
        },
        {
            "_PROCESS_": "reduce",
            "_PARAMS_": {
                "pca_npcs": {"_SWEEP_": {"min": 2, "max": 5, "base": 2}},
                "umap_n_neighbors": {"_SWEEP_": {"min": 2, "max": 5, "step": 1}},
                "umap_min_dist": 0.1,
                "umap_n_components": 2,
                "umap_metric": {"_SWEEP_": ["cosine", "euclidean"]},
            },
        },
    ]
    return spec


def test_run_group_spec(tree_spec):
    norm_group = RunGroupSpec(tree_spec[0])
    reduce_group = RunGroupSpec(tree_spec[1])
    norm_run_specs = norm_group.run_specs
    reduce_run_specs = reduce_group.run_specs
    # 2 samples * ([1 seurat_default * 7 max_genes x 7 nfeatures] + [1 sctransform])
    assert len(norm_run_specs) == 100
    # 4 pca_npcs * 4 * umap_n_neighbors * 2 umap_metric
    assert len(reduce_run_specs) == 32


def test_tree_spec(data_dir, tree_spec):
    tree_spec = TreeSpec(tree_spec)
    assert len(tree_spec.branch_specs) == 3200  # 100 normalization x 32 reduce
    return tree_spec
