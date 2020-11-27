from dataforest.structures.cache.HashCash import HashCash
from dataforest.utils.catalogue import run_id_from_multi_row


class RunIdCache(HashCash):
    """
    Key: process_name

    Val: run_id (hash)
    """

    def __init__(self, spec, process_catalogue_cache):
        super().__init__()
        self._spec = spec
        self._process_catalogue_cache = process_catalogue_cache

    def _get(self, process_name):
        run_spec_str = str(self._spec[process_name])
        run_catalogue = self._process_catalogue_cache[process_name]
        if run_spec_str in run_catalogue.index:
            run_id_rows = run_catalogue.loc[run_spec_str]
            if len(run_id_rows) != 1:
                run_id = run_id_from_multi_row(run_id_rows)
            else:
                run_id = run_id_rows["run_id"]
            if not isinstance(run_id, str):
                raise ValueError()
        else:
            # TODO: test this for silent failures -- should error be raised instead?
            run_id = None
        return run_id
