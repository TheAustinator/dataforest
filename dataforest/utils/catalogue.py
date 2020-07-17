import logging


def run_id_from_multi_row(run_id_rows):
    unique = list(run_id_rows["run_id"].unique())
    run_id = unique[0]
    if len(unique) != 1:
        # TODO implement integrity check and removal
        logging.warning(f"Multiple unique run IDs in catalogue for spec. IDs: {unique}. Using {run_id}")
    else:
        # TODO implement cleanup
        logging.warning(f"Duplicate catalogue entries for run_id {run_id}")
    return run_id
