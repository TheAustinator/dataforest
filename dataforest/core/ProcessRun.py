from collections import ChainMap
import logging
from pathlib import Path
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
from termcolor import cprint


if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch


class ProcessRun:
    """
    Interface to the files, status, logs, and DataBranch at a node representing a run
    of a specific processes.
    """

    def __init__(self, branch: "DataBranch", process_name: str, process: str):
        self._LOG = logging.getLogger(f"ProcessRun - {process_name}")
        if process_name not in branch.spec and process_name != "root":
            raise ValueError(f'key "{process_name}" not in branch_spec: {branch.spec}')
        self.process_name = process_name
        self.process = process
        self._branch = branch
        self._layers_files = self._build_layers_files()
        self._file_lookup = self._build_file_lookup()
        self._plot_lookup = self._build_plot_lookup()
        self._path_map = None
        self._plot_map = None
        self._path_map_prior = None

    @property
    def branch(self) -> "DataBranch":
        """
        DataBranch `at` current `ProcessRun` node, which is cached after initial
        access. If the parent DataBranch has no sample_metadata yet, the access to the
        current node should only be used for paths, as the parent DataBranch is
        returned.
        """
        return self._branch

    @property
    def path(self) -> Path:
        """Path to directory containing processes output files and logs"""
        return self.branch.paths[self.process_name]

    @property
    def logs_path(self) -> Path:
        return self.path / "_logs"

    @property
    def plots_path(self) -> Path:
        return self.path / "_plots"

    @property
    def process_meta(self) -> pd.DataFrame:
        """
        Gets additional sample_metadata created by the current process, which is
        usually used to merge with the overall sample_metadata.
        """
        meta_path = self.path_map["meta"]
        try:
            return pd.read_csv(meta_path, sep="\t", index_col=0)
        except FileNotFoundError:
            raise FileNotFoundError(f'No additional sample_metadata for "{self.process_name}"')

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
    def process_plot_map(self) -> Dict[str, Path]:
        plot_map_dict = {}
        for plot_name in self._plot_lookup:
            plot_map_dict[plot_name] = {}
            for plot_kwargs_key, plot_filepath in self._plot_lookup[plot_name].items():
                plot_map_dict[plot_name][plot_kwargs_key] = self.plots_path / plot_filepath

        return plot_map_dict

    @property
    def path_map(self) -> Dict[str, Path]:
        """
        Path map like `process_path_map`, but including files which are not
        present in the current process run dir, but are from previous
        processes. Gets paths for all aliases, providing the path for the most
        recent file for each in the process lineage.
        """
        if self._path_map is None:
            self._path_map = self._build_path_map(incl_current=True)
        return self._path_map

    @property
    def plot_map(self) -> Dict[str, Path]:
        # TODO: confusing with new plot_map name in config - rename that to plot_settings?
        if self._plot_map is None:
            self._plot_map = self._build_path_map(incl_current=True, plot_map=True)
        return self._plot_map

    @property
    def path_map_prior(self) -> Dict[str, Path]:
        """Like `path_map`, but for excluding the current process"""
        if self._path_map_prior is None:
            self._path_map_prior = self._build_path_map(incl_current=False)
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
        Whether or not process has been executed to completion, regardless of
        success
        """
        if self.path.exists() and not (self.path / "INCOMPLETE").exists():
            if len(self.files) > 0 or self.logs_path.exists():
                return True
        return False

    @property
    def success(self) -> bool:
        """
        Checks whether run is done by checking whether output directory contains non-logging or temp files
        """
        # TODO: make more robust by adding `DONE_REQUIREMENT_FILES` to `ProcessSchema`
        if self.path is not None and self.path.exists():
            if (self.path / "meta.tsv").exists() and not self.failed:
                return True
        return False

    @property
    def failed(self) -> bool:
        if self.path is not None and self.path.exists():
            error_prefixes = ["PROCESS__", "HOOKS__"]
            is_error_file = lambda s: any(map(lambda prefix: s.name.startswith(prefix), error_prefixes))
            contains_error_file = any(map(is_error_file, self.logs_path.iterdir()))
            if contains_error_file:
                return True
        return False

    @property
    def params(self):
        raise NotImplementedError()

    @property
    def logs(self):
        """
        Prints stdout and stderr log files
        """
        log_dir = self.path / "_logs"
        log_files = list(log_dir.iterdir())
        stdouts = list(filter(lambda x: str(x).endswith(".out"), log_files))
        stderrs = list(filter(lambda x: str(x).endswith(".err"), log_files))
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
        """DataFrame of branch_spec info for all runs of a given subprocess"""
        raise NotImplementedError()

    def _build_layers_files(self) -> Dict[str, str]:
        """
        Builds {file_alias: filename} lookup for additional layers specified
        for process in config.
        """
        layers_names = self.branch.schema.__class__.PROCESS_LAYERS.get(self.process, [])
        layers_files_list = [self.branch.schema.__class__.LAYERS[name] for name in layers_names]
        layers_files = dict(ChainMap(*layers_files_list))
        return layers_files

    def _build_file_lookup(self) -> Dict[str, str]:
        """
        Builds {file_alias: filename} lookup which combines config file_map and
        process layers
        """
        file_lookup = self.branch.schema.__class__.FILE_MAP.get(self.process, {})
        file_lookup.update(self._layers_files)
        return file_lookup

    def _build_plot_lookup(self) -> Dict[str, str]:
        return self.branch.schema.__class__.PLOT_MAP.get(self.process, {})

    def _build_path_map(self, incl_current: bool = False, plot_map: bool = False) -> Dict[str, Path]:
        """
        See `path_map` property
        Args:
            incl_current: whether or not to include files from the current process
        """
        spec = self.branch.spec
        precursor_lookup = spec.get_precursors_lookup(incl_current=incl_current, incl_root=True)
        precursors = precursor_lookup[self.process_name]
        process_runs = [self] if plot_map else [self.branch[process_name] for process_name in precursors]
        pr_attr = "process_plot_map" if plot_map else "process_path_map"
        process_path_map_list = [getattr(pr, pr_attr) for pr in process_runs]
        path_map = dict()
        for process_path_map in process_path_map_list:
            path_map.update(process_path_map)
        return path_map

    def __repr__(self):
        repr_ = super().__repr__()[:-1]  # remove closing bracket to append
        repr_ += f" process: {self.process}; process_name: {self.process_name}; done: {self.done}>"
        return repr_
