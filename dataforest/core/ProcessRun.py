import logging
import os
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
from pathlib import Path
from termcolor import cprint

from dataforest.filesystem.core.DataTree import DataTree

if TYPE_CHECKING:
    from dataforest.core.DataForest import DataForest


class ProcessRun:
    """
    Interface to the files, status, logs, and DataForest at a node representing a run
    of a specific processes.
    """

    def __init__(self, forest: "DataForest", process_name: str):
        self.logger = logging.getLogger(f"ProcessRun - {process_name}")
        if process_name not in forest.spec:
            raise ValueError(f"key '{process_name}' not in spec: {forest.spec}")
        self.process_name = process_name
        self._forest = None
        self._parent_forest = forest

    @property
    def forest(self) -> "DataForest":
        """
        DataForest `at` current `ProcessRun` node, which is cached after initial
        access. If the parent DataForest has no metadata yet, the access to the
        current node should only be used for paths, as the parent DataForest is
        returned.
        """
        # TODO: silent failure prone
        if not self._forest:
            self._forest = self._parent_forest.at(self.process_name)
        return self._forest

    @property
    def forest_any(self):
        """quick access if it doesn't matter whether forest or parent forest"""
        return self._forest if self._forest else self._parent_forest

    @property
    def path(self) -> Path:
        """Path to directory containing processes output files and logs"""
        return self.forest_any.paths[self.process_name]

    @property
    def files(self) -> List[str]:
        """File names"""
        return sorted(list(map(lambda x: x.name, self.filepaths)))

    @property
    def filepaths(self) -> List[Path]:
        return [path for path in self.path.iterdir() if path.is_file()]

    @property
    def path_map(self) -> Dict[str, Path]:
        """
        keys (str): processes `file_alias`es (defined in `ProcessSchema`)
        values (Path): paths to files
        """
        return {file_alias: self.path / self._file_lookup[file_alias] for file_alias in self._file_lookup}

    @property
    def file_map(self) -> Dict[str, str]:
        """
        keys: processes `file_alias`es (defined in `ProcessSchema`)
        values: filenames
        """
        return {file_alias: self._file_lookup[file_alias] for file_alias in self._file_lookup}

    @property
    def file_map_done(self) -> Dict[str, str]:
        """
        `ProcessRun.file_map` filtered by existence
        """
        return {alias: filename for alias, filename in self.file_map.items() if (self.path / filename).exists()}

    @property
    def done(self) -> bool:
        """
        Checks whether run is done by checking whether output directory contains non-logging or temp files
        """
        # TODO: make more robust by adding `DONE_REQUIREMENT_FILES` to `ProcessSchema`
        if self.path.exists():
            output_file_check = lambda x: not (x.endswith("out") or x.endswith("err") or x.startswith("temp"))
            output_files = filter(output_file_check, self.files)
            return len(list(output_files)) > 0
        return False

    @property
    def params(self):
        raise NotImplementedError()

    @property
    def logs(self):
        """
        Prints stdout and stderr log files
        """
        stdouts = list(filter(lambda x: str(x).endswith(".out"), self.filepaths))
        stderrs = list(filter(lambda x: str(x).endswith(".err"), self.filepaths))
        if (len(stdouts) == 0) and (len(stderrs) == 0):
            raise ValueError(f"No logs for processes: {self.process_name}")
        for stdout in stdouts:
            name = str(stdout.name).split(".out")[0]
            cprint(f"STDOUT: {name}", "cyan", "on_grey")
            with open(str(stdout), "r") as f:
                print(f.read())
        for stderr in stderrs:
            name = str(stderr.name).split(".err")[0]
            cprint(f"STDERR: {name}", "magenta", "on_grey")
            with open(str(stderr), "r") as f:
                print(f.read())

    def subprocess_runs(self, process_name: str) -> pd.DataFrame:
        """DataFrame of spec info for all runs of a given subprocess"""
        import ipdb

        ipdb.set_trace()
        run_dirs = os.listdir(str(self.path / process_name))
        run_dicts = map(DataTree.from_str, run_dirs)
        df = pd.DataFrame(run_dicts)
        return df

    @property
    def _file_lookup(self):
        file_lookup = self.forest_any.schema.__class__.FILE_MAP[self.process_name]
        standard_files = self.forest_any.schema.__class__.STANDARD_FILES
        file_lookup.update(standard_files)
        return file_lookup
