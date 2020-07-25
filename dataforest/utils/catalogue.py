import logging

import pandas as pd


def run_id_from_multi_row(run_id_rows: pd.DataFrame) -> str:
    """
    Extract the `run_id` from a `run_catalogue` where multiple rows match the
    given `RunSpec`. Note that this shouldn't happen, but in the case of a
    double entry, or two different `run_id`s for the same `RunSpec`, this gets
    the first.
    Args:
        run_id_rows: rows of run_catalogue matching a given `RunSpec`
    """
    unique = list(run_id_rows["run_id"].unique())
    run_id = unique[0]
    if len(unique) != 1:
        # TODO implement integrity check and removal
        logging.warning(f"Multiple unique run IDs in catalogue for spec. IDs: {unique}. Using {run_id}")
    else:
        # TODO implement cleanup
        logging.warning(f"Duplicate catalogue entries for run_id {run_id}")
    return run_id
