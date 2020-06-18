import logging
import os
from pathlib import Path
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
from termcolor import cprint

from dataforest.DataTree import DataTree

if TYPE_CHECKING:
    from dataforest.ORM import DataForest


class ProcessRun:
    """
    Interface to the files, status, logs, and orm at a node representing a run
    of a specific process.
    """

    def __init__(self, orm: "DataForest", process_name: str):
        self.logger = logging.getLogger(f"ProcessRun - {process_name}")
        if process_name not in orm.spec:
            raise ValueError(f"key '{process_name}' not in spec: {orm.spec}")
        self.process_name = process_name
        self._orm = None
        self._parent_orm = orm

    @property
    def orm(self) -> "DataForest":
        """
        ORM `at` current `ProcessRun` node, which is cached after initial
        access. If the parent ORM has no metadata yet, the access to the
        current node should only be used for paths, as the parent ORM is
        returned.
        """
        # TODO: silent failure prone
        if not self._orm:
            self._orm = self._parent_orm.at(self.process_name)
        return self._orm

    @property
    def orm_any(self):
        """orm if loaded, otherwise parent orm. For simple things like paths"""
        return self._orm if self._orm else self._parent_orm

    @property
    def path(self) -> Path:
        """Path to directory containing process output files and logs"""
        return self.orm_any.paths[self.process_name]

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
        keys (str): process `file_alias`es (defined in `ProcessSchema`)
        values (Path): paths to files
        """
        file_lookup = self.orm_any.schema.FILE_MAP[self.process_name]
        return {file_alias: self.path / file_lookup[file_alias] for file_alias in file_lookup}

    @property
    def file_map(self) -> Dict[str, str]:
        """
        keys: process `file_alias`es (defined in `ProcessSchema`)
        values: filenames
        """
        file_lookup = self.orm_any.schema.FILE_MAP[self.process_name]
        return {file_alias: file_lookup[file_alias] for file_alias in file_lookup}

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
            raise ValueError(f"No logs for process: {self.process_name}")
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
