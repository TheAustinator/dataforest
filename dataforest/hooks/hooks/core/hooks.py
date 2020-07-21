import gc
import logging
from pathlib import Path

import pandas as pd
import yaml

from dataforest.utils.catalogue import run_id_from_multi_row
from dataforest.utils.exceptions import InputDataNotFound
from dataforest.hooks.hook import hook


@hook
def hook_goto_process(dp):
    dp.forest.goto_process(dp.name)


@hook(attrs=["comparative"])
def hook_comparative(dp):
    """Sets up DataForest for comparative analysis"""
    if "partition" in dp.forest.spec:
        logging.warning(
            "`partition` found at base level of spec. It should normally be specified under an individual processes"
        )

    if dp.comparative:
        partition = dp.forest.spec[dp.name].get("partition", None)
        if partition is None:
            example_dict = {dp.name: {"partition": {"var_1", "var_2"}}}
            raise ValueError(
                f"When `dataprocess` arg `comparative=True`, `forest.spec` must contain the key "
                f"'partition' nested inside the decorated processes name. I.e.: {example_dict}"
            )
        dp.forest.set_partition(dp.name)


@hook(attrs=["requires"])
def hook_input_exists(dp):
    """Checks that input `ProcessRun` directory exists"""
    if not dp.forest.paths_exists[dp.requires].exists():
        raise InputDataNotFound(dp, dp.requires, dp.name)
    contains_files = any(list(map(Path.is_file, dp.forest.paths_exists[dp.requires].iterdir())))
    if not contains_files:
        raise InputDataNotFound(dp.forest, dp.requires, dp.name)


@hook
def hook_mkdirs(dp):
    """Setup hook that makes directories for `ProcessRun` outputs"""
    process_path = dp.forest.paths_exists.get_process_dir(dp.name)
    if not process_path.exists():
        process_path.mkdir(parents=True, exist_ok=True)
    run_path = dp.forest.paths_exists[dp.name]
    if not run_path.exists():
        run_path.mkdir(parents=True, exist_ok=True)


@hook
def hook_overwrite(dp):
    # TODO: fill in
    raise NotImplementedError()


@hook
def hook_garbage_collection(dp):
    gc.collect()


@hook
def hook_store_run_spec(dp):
    """Store `RunSpec` as yaml in process run directory"""
    run_spec = dp.forest.spec[dp.name]
    run_path = dp.forest.paths_exists[dp.name]
    run_spec_path = run_path / "run_spec.yaml"  # TODO: hardcoded
    with open(run_spec_path, "w") as f:
        yaml.dump(dict(run_spec), f)


@hook
def hook_catalogue(dp):
    """
    Updates `run_catalogue.tsv` in enclosing process dir with current process
    run. Adds a row with the `run_id` (hash), which serves as the process run
    directory name.
    If there's an existing entry for the current `RunSpec`, ensures that the
    current `run_id` matches that stored, otherwise, raises an exception
    """
    run_spec = dp.forest.spec[dp.name]
    run_spec_str = str(run_spec)
    run_id = dp.forest.paths_exists.get_run_id(dp.name)
    process_dir = dp.forest.paths_exists.get_process_dir(dp.name)
    catalogue_path = process_dir / "run_catalogue.tsv"
    df = pd.read_csv(catalogue_path, sep="\t", index_col="run_spec")
    if str(run_spec) not in df.index:
        df = df.append({"run_spec": run_spec_str, "run_id": run_id}, ignore_index=True)
        df.to_csv(catalogue_path, sep="\t", index=False)
    else:
        run_id_rows = df.loc[str(run_spec)]["run_id"]
        if not isinstance(run_id_rows, str):
            run_id_stored = run_id_from_multi_row(run_id_rows)
            if run_id != run_id_stored:
                raise ValueError(f"run_id: {run_id} is not equal to stored: {run_id_stored} for {str(run_spec)}")
