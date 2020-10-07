from copy import deepcopy
import gc
import logging
from pathlib import Path
import shutil

import pandas as pd
import yaml

from dataforest.hooks.dataprocess import dataprocess
from dataforest.utils.catalogue import run_id_from_multi_row
from dataforest.utils.exceptions import InputDataNotFound
from dataforest.hooks.hook import hook


@hook
def hook_goto_process(dp):
    dp.branch.goto_process(dp.name)


@hook(attrs=["comparative"])
def hook_comparative(dp):
    """Sets up DataBranch for comparative analysis"""
    if "_PARTITION_" in dp.branch.spec:
        logging.warning(
            "`partition` found at base level of branch_spec. It should normally be specified under an individual "
            "processes"
        )

    if dp.comparative:
        partition = dp.branch.spec[dp.name].get("_PARTITION_", None)
        if partition is None:
            example_dict = {dp.name: {"_PARTITION_": {"var_1", "var_2"}}}
            raise ValueError(
                f"When `dataprocess` arg `comparative=True`, `branch.spec` must contain the key "
                f"'partition' nested inside the decorated processes name. I.e.: {example_dict}"
            )
        dp.branch.set_partition(dp.name)


@hook(attrs=["requires"])
def hook_input_exists(dp):
    """Checks that input `ProcessRun` directory exists"""
    if not dp.branch.paths_exists[dp.requires].exists():
        raise InputDataNotFound(dp, dp.requires, dp.name)
    contains_files = any(list(map(Path.is_file, dp.branch.paths_exists[dp.requires].iterdir())))
    if not contains_files:
        raise InputDataNotFound(dp.branch, dp.requires, dp.name)


@hook
def hook_mkdirs(dp):
    """Setup hook that makes directories for `ProcessRun` outputs"""
    process_path = dp.branch.paths_exists.get_process_dir(dp.name)
    process_path.mkdir(parents=True, exist_ok=True)
    run_path = dp.branch.paths[dp.name]
    run_path.mkdir(parents=True, exist_ok=True)
    if hasattr(dp, "plots") and dp.plots:
        plots_dir = run_path / "_plots"
        plots_dir.mkdir(exist_ok=True)


@hook
def hook_mark_incomplete(dp):
    token_path = dp.branch[dp.name].path / "INCOMPLETE"
    try:
        token_path.touch(exist_ok=True)
    except Exception as e:
        raise e


@hook
def hook_mark_complete(dp):
    token_path = dp.branch[dp.name].path / "INCOMPLETE"
    token_path.unlink(missing_ok=True)


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
    run_spec = dp.branch.spec[dp.name]
    run_path = dp.branch.paths_exists[dp.name]
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
    run_spec = dp.branch.spec[dp.name]
    run_spec_str = str(run_spec)
    run_id = dp.branch.paths_exists.get_run_id(dp.name)
    process_dir = dp.branch.paths_exists.get_process_dir(dp.name)
    catalogue_path = process_dir / "run_catalogue.tsv"
    df = pd.read_csv(catalogue_path, sep="\t", index_col="run_spec")
    if str(run_spec) not in df.index:
        new_entry = pd.DataFrame({"run_id": [run_id]}, index=[run_spec_str])
        new_entry.index.name = "run_spec"
        df = df.append(new_entry)
        df.to_csv(catalogue_path, sep="\t")
    else:
        run_id_rows = df.loc[str(run_spec)]["run_id"]
        if not isinstance(run_id_rows, str):
            run_id_stored = run_id_from_multi_row(run_id_rows)
            if run_id != run_id_stored:
                raise ValueError(f"run_id: {run_id} is not equal to stored: {run_id_stored} for {str(run_spec)}")


# TODO-QC: take a check here
@hook
def hook_generate_plots(dp: dataprocess):
    plot_sources = dp.branch.plot.plot_method_lookup
    current_process = dp.branch.current_process
    all_plot_kwargs_sets = dp.branch.plot.plot_kwargs[current_process]
    process_plot_methods = dp.branch.plot.plot_methods[current_process]
    process_plot_map = dp.branch[dp.branch.current_process].plot_map
    requested_plot_methods = deepcopy(process_plot_methods)

    for method in plot_sources.values():
        plot_method_name = method.__name__
        if plot_method_name in requested_plot_methods.values():
            plot_name = dp.branch.plot.method_key_lookup[plot_method_name]
            plot_kwargs_sets = all_plot_kwargs_sets[plot_name]
            for plot_kwargs_key in plot_kwargs_sets.keys():
                plot_path = process_plot_map[plot_name][plot_kwargs_key]
                kwargs = deepcopy(plot_kwargs_sets[plot_kwargs_key])
                kwargs["plot_path"] = plot_path
                method(dp.branch, **kwargs)
            requested_plot_methods.pop(plot_name)

    if len(requested_plot_methods) > 0:  # if not all requested mapped to functions in plot sources
        logging.warning(
            f"Requested plotting methods {requested_plot_methods} are not implemented so they were skipped."
        )


@hook
def hook_clear_logs(dp: dataprocess):
    logs_path = dp.branch[dp.name].logs_path
    if logs_path.exists():
        shutil.rmtree(str(logs_path), ignore_errors=True)
