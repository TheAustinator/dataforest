import base64
import os
from copy import copy
from pathlib import Path
from typing import Optional

from dataforest.structures.cache.HashCash import HashCash
from dataforest.core.Spec import Spec
from dataforest.structures.cache.RunCatalogueCache import RunCatalogCache
from dataforest.structures.cache.RunIdCache import RunIdCache


class PathCache(HashCash):
    """
    Lazy loading path lookup by process_name. If the `ProcessRun` has not yet
    been executed, a `run_id` is generated. Due to non-determinism of `run_id`
    hashes, paths can only be calculated up to one process ahead of what has
    been executed.

    Key (str): process name

    Val (Optional[Path]): path to process run matching spec
        `process_run_path` = `process_path` / `run_id`
    """

    def __init__(self, root_dir: Path, spec: Spec, exists_req: bool):
        super().__init__()
        self._cache["root"] = root_dir
        self._spec = spec
        self._exists_req = exists_req
        self._process_catalogue_cache = RunCatalogCache(self.get_process_dir, self._spec)
        self._run_id_cache = RunIdCache(self._spec, self._process_catalogue_cache)

    def keys(self):
        run_spec_names = {run_spec.name for run_spec in self._spec}
        cached_keys = set(self._cache.keys())
        return list(cached_keys.union(run_spec_names))

    def get_process_dir(self, process_name: str) -> Path:
        """
        Process directory containing process runs, which is not specific to any
        specific process run.
        """
        precursor_list = self._spec.get_precursors_lookup(incl_root=True)[process_name]
        if precursor_list:
            precursor = precursor_list[-1]
        else:
            raise ValueError('`precursor_list` is empty, should be at least ["root"]')
        precursor_path = self[precursor]
        if precursor_path is None:
            # TODO: update with some sort of path
            raise FileNotFoundError(f"No existing path found for process: {precursor}")
        return precursor_path / process_name

    def get_run_id(self, process_name: str) -> str:
        return self._run_id_cache[process_name]

    def get_shared_memory_view(self, exist_req: bool):
        """
        Get a shared memory accessor with option to modify `exist_req`
        """
        view = copy(self)
        view._exists_req = exist_req
        return view

    def _get(self, process_name: str) -> Optional[Path]:
        """
        Gets `process_run_path` if `process_path`. Attempts
        to retrieve the `run_id` it from `run_catalogue`, and generates it if
        one doesn't exist.
        """
        process_dir = self.get_process_dir(process_name)
        if self._exists_req and not process_dir.exists():
            return None
        run_id = self.get_run_id(process_name)
        if run_id is None:
            run_id = base64.urlsafe_b64encode(os.urandom(128))[:8].decode()
            run_id = run_id.replace("_", "a")  # "_" is reserved character
            self._run_id_cache[process_name] = run_id
        process_run_dir = process_dir / run_id
        return process_run_dir
