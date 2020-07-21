import logging
from pathlib import Path
from typing import Callable

import pandas as pd
import yaml

from dataforest.core.Spec import Spec
from dataforest.structures.cache.HashCache import HashCash


class RunCatalogCache(HashCash):
    """
    Lazy loading lookup for run_catalogue dataframes by process.

    Key (str): process name

    Val (Optional[pd.DataFrame]): The run catalogue serves as a lookup to find
        the directory hash for a given `RunSpec`.
    """

    # TODO: to make this more readable, rather than storing a JSON string, actually make table of params, etc
    #  problem: nesting? - could just give params their own cols, then prefix any data ops with thier type
    #  (e.g "subset_donor"), or just leave data ops as blobs
    RUN_SPEC_FILENAME = "run_spec.yaml"

    def __init__(self, get_process_dir: Callable, spec: Spec):
        super().__init__()
        self.get_process_dir = get_process_dir
        self._spec = spec

    def keys(self):
        run_spec_names = {run_spec.name for run_spec in self._spec}
        cached_keys = set(self._cache.keys())
        return list(cached_keys.union(run_spec_names))

    def _get(self, process_name: str) -> pd.DataFrame:
        """
        Attempts to load existing run catalogue from the `process_path` of
        `process_name`, and `_build`s one if none exists.
        """
        process_dir = self.get_process_dir(process_name)
        catalogue_path = process_dir / "run_catalogue.tsv"  # TODO: hardcoded
        if catalogue_path.exists():
            df = pd.read_csv(catalogue_path, sep="\t", index_col="run_spec")
        else:
            df = self._build(process_dir)
        return df

    @staticmethod
    def _build(process_dir: Path) -> pd.DataFrame:
        """
        Builds `run_catalogue` from any existing process runs in a
        `process_path`
        """
        # TODO: make function to merge during push
        run_spec_filename = RunCatalogCache.RUN_SPEC_FILENAME
        process_dir = Path(process_dir)
        process_dir.mkdir(parents=True, exist_ok=True)
        catalogue_dict = dict()
        for process_run_path in process_dir.glob("*"):
            if process_run_path.is_file():
                continue
            run_spec_path = process_run_path / run_spec_filename
            if not run_spec_path.exists():
                logging.warning(f"No `{run_spec_filename}` in process run directory: {process_run_path}")
                continue
            run_id = process_run_path.name
            with open(str(run_spec_path), "r") as f:
                run_spec = yaml.load(f, yaml.FullLoader)
            run_spec_str = str(run_spec)
            catalogue_dict[run_spec_str] = run_id
        columns = ["run_spec", "run_id"]
        df = pd.DataFrame(catalogue_dict, columns=columns)
        catalogue_path = process_dir / "run_catalogue.tsv"  # TODO: hardcoded
        df.to_csv(catalogue_path, index=False, sep="\t")
        return df
