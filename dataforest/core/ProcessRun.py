from collections import ChainMap
import logging
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
from pathlib import Path
from termcolor import cprint


if TYPE_CHECKING:
    from dataforest.core.DataForest import DataForest


class ProcessRun:
    """
    Interface to the files, status, logs, and DataForest at a node representing a run
    of a specific processes.
    """

    def __init__(self, forest: "DataForest", process_name: str, process: str):
        self.logger = logging.getLogger(f"ProcessRun - {process_name}")
        if process_name not in forest.spec and process_name != "root":
            raise ValueError(f"key '{process_name}' not in spec: {forest.spec}")
        self.process_name = process_name
        self.process = process
        self._forest = forest
        self._layers_files = self._build_layers_files()
        self._file_lookup = self._build_file_lookup()
        self._path_map = None
        self._path_map_prior = None

    @property
    def forest(self) -> "DataForest":
        """
        DataForest `at` current `ProcessRun` node, which is cached after initial
        access. If the parent DataForest has no metadata yet, the access to the
        current node should only be used for paths, as the parent DataForest is
        returned.
        """
        return self._forest

    @property
    def path(self) -> Path:
        """Path to directory containing processes output files and logs"""
        return self.forest.paths[self.process_name]

    @property
    def process_meta(self):
        """
        Gets additional metadata created by the current process, which is
        usually used to merge with the overall metadata.
        """
        meta_path = self.path_map["meta"]
        df = None
        if meta_path.exists():
            df = pd.read_csv(meta_path, sep="\t", index_col=0)
        return df

    @property
    def files(self) -> List[str]:
        """File names"""
        return sorted(list(map(lambda x: x.name, self.filepaths)))

    @property
    def filepaths(self) -> List[Path]:
        return [path for path in self.path.iterdir() if path.is_file()]

    @property
    def process_path_map(self) -> Dict[str, Path]:
        """
        keys (str): processes `file_alias`es (defined in `ProcessSchema`)
        values (Path): paths to files
        """
        return {file_alias: self.path / self._file_lookup[file_alias] for file_alias in self._file_lookup}

    @property
    def path_map(self) -> Dict[str, Path]:
        """
        Path map like `process_path_map`, but including files which are not
        present in the current process run dir, but are from previous
        processes. Gets paths for all aliases, providing the path for the most
        recent file for each in the process lineage.
        """
        if self._path_map is None:
            self._path_map = self._build_path_map(incl_curr=True)
        return self._path_map

    @property
    def path_map_prior(self) -> Dict[str, Path]:
        """Like `path_map`, but for excluding the current process"""
        if self._path_map_prior is None:
            self._path_map_prior = self._build_path_map(incl_curr=False)
        return self._path_map_prior

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
        if self.path is not None and self.path.exists():
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
        raise NotImplementedError()

    def _build_layers_files(self) -> Dict[str, str]:
        """
        Builds {file_alias: filename} lookup for additional layers specified
        for process in config.
        """
        layers_names = self.forest.schema.__class__.PROCESS_LAYERS.get(self.process, [])
        layers_files_list = [self.forest.schema.__class__.LAYERS[name] for name in layers_names]
        layers_files = dict(ChainMap(*layers_files_list))
        return layers_files

    def _build_file_lookup(self) -> Dict[str, str]:
        """
        Builds {file_alias: filename} lookup which combines config file_map and
        process layers
        """
        file_lookup = self.forest.schema.__class__.FILE_MAP.get(self.process, {})
        file_lookup.update(self._layers_files)
        return file_lookup

    def _build_path_map(self, incl_curr: bool = False) -> Dict[str, Path]:
        """
        See `path_map` property
        Args:
            incl_curr: whether or not to include files from the current process
        """
        spec = self.forest.spec
        precursor_lookup = spec.get_precursors_lookup(incl_current=incl_curr, incl_root=True)
        precursors = precursor_lookup[self.process_name]
        process_runs = [self.forest[process_name] for process_name in precursors]
        process_path_map_list = [pr.process_path_map for pr in process_runs]
        path_map = dict()
        for process_path_map in process_path_map_list:
            path_map.update(process_path_map)
        return path_map

    def __repr__(self):
        repr_ = super().__repr__()[:-1]  # remove closing bracket to append
        repr_ += f" process: {self.process}; process_name: {self.process_name}; done: {self.done}>"
        return repr_
