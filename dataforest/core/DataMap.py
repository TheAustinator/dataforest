import logging
import os
from typing import List, Optional, Union

from pathlib import Path

from dataforest.filesystem.DataTree import DataTree
from dataforest.templates.ProcessSchema import ProcessSchema


class DataMap(list):
    """
    Map of all existing output data from `process_run`s on filesystem.
    Extracts and stores all previous `process_run` specifications directly from
    filesystem directory structure. Converts the `ForestQuery` syntax of
    `process_run` directories to data and process specifications that were used
    for that run. Assumes directory structure which adheres to alternating
    `process_name`s and `process_run`s.
    Attributes:
        logger: attached logger
        schema: definition of process hierarchy and expected files therewithin
    """

    def __init__(self, root_dir: Union[Path, str], schema: ProcessSchema):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.root_dir = root_dir
        self.schema = schema
        # TODO: too computationally intensive -- run only in cases of errors
        #  and run only one or two levels deep
        # try:
        #     paths = list(Path(root_dir).rglob("*"))
        # except FileNotFoundError as e:
        #     self.logger.warning(f"Cannot build DataMap: {e}")
        #     paths = list()
        # super().__init__(self._assemble_map(paths))
        super().__init__()

    def _assemble_map(self, paths: List[Path]) -> List[dict]:
        """
        Extracts relative paths of each `process_run` directory and converts
        them to dictionaries in `data_forest.Spec` format.
        Args:
            paths: paths from which to assemble map -- usually all paths from
                `schema`s root directory

        Returns:
            branches: list of existing `process_run` data and parameter
                specifications
        """
        dir_paths = [path for path in paths if not path.is_file()]
        rel_paths = [os.path.relpath(str(path), self.root_dir) for path in dir_paths]
        split_paths = [path.split("/") for path in rel_paths]
        data_paths = [path for path in split_paths if len(path) % 2 == 0]
        branches = list()
        for path_elems in data_paths:
            branch = self._assemble_branch(path_elems)
            if branch is not None:
                branches.append(branch)
        return branches

    def _assemble_branch(self, path_elems: List[str]) -> Optional[dict]:
        """
        Use `DataTree.simple` to create a `Spec` like `dict`, which specifies
        the series of `process_run` conditions which yielded the data in the
        `process_run` directory specified by `path_elems`. It takes the general
        form shown below.
        Example:
            {process_name: {
                process_param: value,
                ...},
            ...,
            filter: ...,
            partition: ...,
            }
        Args:
            path_elems: list of individual directory names which describe
                directory relative path

        Returns:
            branch: `Spec` like `dict` described above
        """
        branch = dict()
        process_names = path_elems[::2]
        process_param_strs = path_elems[1::2]
        for (name, params_str) in set(zip(process_names, process_param_strs)):
            if self._process_name_contains_operators(name, path_elems):
                return None
            param_names = self.schema.PARAM_NAMES[name]
            # TODO: figure out how to handle limiting constraints of `simple`
            params = DataTree.from_str(params_str, param_names).simple
            branch[name] = params
        return branch

    def _process_name_contains_operators(self, name, path_elems) -> bool:
        """
        Enforces alternating `process_name`/`process_run` directory structure
        by ensuring that all directories names presumed to be `process_name`
        don't contain any `ForestQuery` operators.
        Args:
            name: directory name which is presumed to be a `process_name`
            path_elems: entire relative path for logging in case assumption is
                broken

        Returns:

        """
        # TODO: currently, if there are two sequential params directories with no process directory separating, the
        #  first one will still be included
        operators_in_name = [op for op in DataTree.OPERATORS if op in name]
        if any(operators_in_name):
            rel_path = "/".join(path_elems)
            self.logger.warning(
                f"WARNING: Path excluded for breaking alternating process/params pattern: {rel_path}"
            )
            return True
        return False
