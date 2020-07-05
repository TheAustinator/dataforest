import gc
import logging
from pathlib import Path

from dataforest.utils.exceptions import InputDataNotFound


def hook_get_process_forest(dp):
    dp.forest = dp.forest.at(dp.process_name)


def hook_comparative(dp):
    """Sets up DataForest for comparative analysis"""
    if "partition" in dp.forest.spec:
        logging.warning(
            "`partition` found at base level of spec. It should normally be specified under an individual processes"
        )

    if dp.comparative:
        partition = dp.forest.spec[dp.process_name].get("partition", None)
        if partition is None:
            example_dict = {dp.process_name: {"partition": {"var_1", "var_2"}}}
            raise ValueError(
                f"When `dataprocess` arg `comparative=True`, `forest.spec` must contain the key "
                f"'partition' nested inside the decorated processes name. I.e.: {example_dict}"
            )
        dp.forest.set_partition(dp.process_name)


def hook_input_exists(dp):
    """Checks that input `ProcessRun` directory exists"""
    if not dp.forest.paths[dp.requires].exists():
        raise InputDataNotFound(dp, dp.requires, dp.process_name)
    contains_files = any(list(map(Path.is_file, dp.forest.paths[dp.requires].iterdir())))
    if not contains_files:
        raise InputDataNotFound(dp.forest, dp.requires, dp.process_name)


def hook_mkdirs(dp):
    """Setup hook that makes directories for `ProcessRun` outputs"""
    path = dp.forest.paths[dp.process_name]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def hook_overwrite(dp):
    # TODO: fill in
    raise NotImplementedError()


def hook_garbage_collection(dp):
    gc.collect()
