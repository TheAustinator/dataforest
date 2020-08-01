import logging
from copy import deepcopy
from typing import Callable, Dict, Optional, Type, Union, List, Iterable, Tuple, Any

import pandas as pd
from pathlib import Path

from dataforest.structures.cache.PathCache import PathCache
from dataforest.structures.cache.IOCache import IOCache
from dataforest.core.ProcessRun import ProcessRun
from dataforest.core.BranchSpec import BranchSpec
from dataforest.filesystem.tree.Tree import Tree

# from dataforest.hyperparams.HyperparameterMethods import HyperparameterMethods
# from dataforest.processes.core.BatchMethods import BatchMethods
from dataforest.plot.PlotMethods import PlotMethods
from dataforest.processes.core.ProcessMethods import ProcessMethods
from dataforest.core.schema.ProcessSchema import ProcessSchema
from dataforest.filesystem.io import ReaderMethods
from dataforest.filesystem.io.WriterMethods import WriterMethods
from dataforest.utils.exceptions import BadSubset, BadFilter
from dataforest.utils.loaders.update_config import update_config
from dataforest.utils.utils import update_recursive


class DataBranch:
    """
    # TODO: update
    NOTE: OUTDATED DOCSTRING
    Core interface for package, which is used to access hierarchical data
    output by chains of processes, whose `process_run`s can be described by the
    processes `params` and input data. At the root level processes, the input data
    is specified by `params`, and in subsequent processes, the input data is
    specified by `subset`s, `filter`s, and `partition`s derived from that initial
    data. `subset`s specify criteria for inclusion, `filter`s specify criteria
    for exclusion, and `partition`s specify criteria for partitioning in
    comparative analyses.
    >>> root = "/data/root:dataset_1"
    >>> branch_spec = {
    >>>     "process_1":
    >>>         {
    >>>             "epsilon": .1,    # param
    >>>             "batch": {3, 4, 5},    # subset
    >>>             "_FILTER_": {"month": {"march", "april"}}    # filter
    >>>         },
    >>>     "process_2": {
    >>>             "alpha": 0.05,    # param
    >>>             "_PARTITION_": "treatment",    # partition
    >>>         }
    >>> }
    >>> branch = DataBranch(root, branch_spec)

    Class Attributes:
        {READER, WRITER}_METHODS: Pointers to container classes for
            reader and writer methods. The methods names should correspond to
            the input `filename` extension, punctuated by `_` in place of `.`,
            excluding the `.` which separates the filename from the extension.
            In case this alone is insufficient, `{READER, WRITER}_KWARGS_MAP`
            can be used to individually map keyword arguments to `file_alias`es
            and {READER, WRITER}_MAP can be used to map methods to individual
            `file_alias`es.

        {READER, WRITER}_MAP: optional overloads for any files for which the
            {reader, writer} from `{READER, WRITER}_METHODS` which was selected
            by `_map_default_{reader, writer}_methods` is not appropriate. Keys
            are `file_alias`es, and values are methods which take `filename`s
            and `kwargs`.

        {READER, WRITER}_KWARGS_MAP: optional overloads for any file for which
            the default keyword arguments to the {reader, writer} should be
            modified. Keys are `file_alias`es and values are `dict`s specifying
            keyword args.

    Dynamically Generated Attributes:
        f_{file_alias}:
        write_{file_alias}:
        _cache_{file_alias}:

    Attributes:
        _paths_exists (PathCache): list of all existing paths through hierarchical
            processes filesystem as dicts in `BranchSpec` format.
        logger:
        schema:
        branch_spec:
        _io_map:
        _reader_kwargs_map:
        _reader_method_map:
        _writer_kwargs_map:
        _writer_method_map:
    """

    PLOT_METHODS: Type = PlotMethods
    PROCESS_METHODS: Type = ProcessMethods
    SCHEMA_CLASS: Type = ProcessSchema
    READER_METHODS: Type = ReaderMethods
    WRITER_METHODS: Type = WriterMethods
    # BATCH_METHODS: Type = BatchMethods
    DATA_FILE_ALIASES: set = set()
    # HYPERPARAMETER_METHODS: Type = HyperparameterMethods
    READER_MAP: dict = dict()
    WRITER_MAP: dict = dict()
    READER_KWARGS_MAP: dict = dict()
    WRITER_KWARGS_MAP: dict = dict()
    _METADATA_NAME: dict = NotImplementedError("Should be implemented by superclass")
    _COPY_KWARGS: dict = {
        "root": "root",
        "branch_spec": "branch_spec",
        "verbose": "verbose",
        "current_process": "current_process",
    }
    _DEFAULT_CONFIG = Path(__file__).parent / "config/default_config.yaml"

    def __init__(
        self,
        root: Union[str, Path],
        branch_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        current_process: Optional[str] = None,
        remote_root: Optional[Union[str, Path]] = None,
    ):
        self._meta = None
        self._unversioned = False
        self._current_process = current_process
        self._remote_root = remote_root
        self.root = Path(root)

        self.branch_spec = self._init_spec(branch_spec)
        self.verbose = verbose
        self.logger = logging.getLogger(self.__class__.__name__)
        self.plot = self.PLOT_METHODS(self)
        self.process = self.PROCESS_METHODS(self, self.branch_spec)
        # self.hyper = HyperparameterMethods(self)
        self.schema = self.SCHEMA_CLASS()
        self._paths_exists = PathCache(self.root, self.branch_spec, exists_req=True)
        self._paths = self._paths_exists.get_shared_memory_view(exist_req=False)
        self._process_runs = dict()
        # TODO:
        self.f, self.w = self._map_file_io()

    @property
    def current_process(self):
        """
        The name of the process from `branch_spec`, which attributes like `meta` and
        `rna` correspond to. For example, these data may be e.g. subset or
        directly modified as a result of some process. Use `goto_process` to
        change this property.
        """
        return self._current_process

    @property
    def remote_root(self) -> Optional[Union[str, Path]]:
        """
        Location of remote root to push to. Not required, only if you'd like to
        sync between local and remote.
        """
        return self._remote_root

    @remote_root.setter
    def remote_root(self, val):
        self._remote_root = val

    def goto_process(self, process_name: str) -> "DataBranch":
        """
        Updates the state of the `DataBranch` object to reflect the
        `ProcessRun` of `process_name` by:
            - clearing metadata cache so that it can be recalculated to include
                only the metadata up to and including that for `process_name`
            - updates the data operations (`subset`, `filter`, `partition`)
            - clears cached data for attributes specified in
                `DATA_FILE_ALIASES` so that it may be recalculated
                appropriately
            process_name: as specified in `branch_spec` under the key `alias` if
                present, or `process` if not
        """

        if self.current_process != process_name:
            prev_path_map = self[self.current_process].path_map if self.current_process else {}
            if self.unversioned:
                self.logger.warning("Calling `at` on unversioned `DataBranch`")
            self._current_process = process_name
            self._meta = None
            new_path_map = self[self.current_process].path_map
            path_map_changes = {alias for alias, path in new_path_map.items() if prev_path_map.get(alias, None) != path}
            for file_alias in path_map_changes:
                if file_alias in self.DATA_FILE_ALIASES:
                    data_attr = f"_{file_alias}"
                    setattr(self, data_attr, None)
        return self

    @classmethod
    def load(cls, root, **kwargs):
        return cls(root, **kwargs)

    @classmethod
    def from_input_dirs(
        cls,
        root: Union[str, Path],
        input_paths: Optional[Union[str, Path, Iterable[Union[str, Path]]]] = None,
        mode: Optional[str] = None,
        **kwargs,
    ) -> "DataBranch":
        """
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataBranch is instantiated.
        The input directories are specified either via
        Args:
            input_paths: list of input data directories
            root: root directory to deposit combined files
        """
        if not isinstance(input_paths, (list, tuple)):
            input_paths = [input_paths]
        additional_kwargs = cls._combine_datasets(root, input_paths=input_paths, mode=mode)
        kwargs = {**additional_kwargs, **kwargs}
        return cls(root, **kwargs)

    @classmethod
    def from_sample_metadata(
        cls, root: Union[str, Path], metadata: Optional[pd.DataFrame] = None, **kwargs,
    ) -> "DataBranch":
        """
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataBranch is instantiated.
        The input directories are specified either via
        Args:
            root: root directory to deposit combined files
            metadata: path to metadata, where each row corresponds to a
                single dataset from `input_dirs`. The only column which must
                be present is `path`, which must be matched to the elements of
                `input_dirs`
        """
        additional_kwargs = cls._combine_datasets(root, metadata=metadata)
        kwargs = {**additional_kwargs, **kwargs}
        return cls(root, **kwargs)

    @property
    def meta(self) -> pd.DataFrame:
        """
        Interface for cell metadata, which is derived from the sample
        metadata and the scrnaseq experimental data. Available UMAP embeddings
        and cluster identifiers will be included, and the data will be subset,
        filtered, and partitioned based on the specifications in `self.branch_spec`.
        Primarily for this reason, this is the preferred interface to metadata
        over direct file access.
        """
        if self._meta is None:
            self._meta = self._get_meta(self.current_process)
        return self._meta

    def fork(self, branch_spec: Union[list, BranchSpec]) -> "DataBranch":
        """
        "Forks" the current branch to create a copy with an altered branch_spec, but
        the same arguments otherwise.
        Args:
            branch_spec: new branch_spec with which the "forked" branch is to be created

        Returns:
            branch: new branch
        """
        branch = self.copy(branch_spec=branch_spec)
        return branch

    def pull(self, local_root: Union[str, Path]) -> "DataBranch":
        """
        Copies a branch to a local root directory, reconciling with existing
        local data.
        Args:
            local_root: local root to which

        Returns:
            branch: new branch with the `root` attribute changed to the local
                root and `remote_root` set to the prior `root`
        """
        self._merge(local_root)

    def push(self, remote_root: Optional[Union[str, Path]]):
        """
        Pushes a branch to a remote root directory, reconciling with existing
        remote data.
        Args:
            remote_root: path to remote dir. Should be mounted path
                (e.g. `/s3/sequencing/my_root`).
        """
        remote_root = remote_root if remote_root else self.remote_root
        if not remote_root:
            # TODO: make version control error
            raise ValueError(
                f"No remote root path to push to provided. Either `DataBranch.remote_root` must be set, or "
                f"`remote_root` must be passed to `DataBranch.push`"
            )
        self._merge(remote_root)

    def _merge(self, root_into: Union[str, Path]):
        """
        Merges current branch into the the DataForest (set of branches) at a
        different root directory. Any conflicting data and `run_catalogue`s
        entries are overwritten
        Args:
            root_into: root directory other than `self.root`, into which
                the current branch's data is to be merged.
        """
        root_into = Path(root_into)
        self._check_root_meta_match(root_into)

        # TODO: if root metadata, check that it's the same
        # TODO: goto each process in chain and get file map
        # TODO: update `done` to use something besides logs
        # TODO: copy file map, evaluate all elements for `done` processes
        # TODO: reconcile and register each process in run_catalogue
        # TODO: replace roots with `remote_root`
        # TODO: copy files

        raise NotImplementedError()

    def copy(self, **kwargs):
        return self.__class__(**kwargs)

    def copy_legacy(self, **kwargs) -> "DataBranch":
        base_kwargs = self._get_copy_base_kwargs()
        kwargs = {**base_kwargs, **kwargs}
        kwargs = {k: deepcopy(v) for k, v in kwargs.items()}
        return self.__class__(**kwargs)

    @property
    def meta(self) -> pd.DataFrame:
        raise NotImplementedError("Should be implemented by subclass")

    def set_meta(self, df: Optional[pd.DataFrame]):
        self._meta = df

    @property
    def paths(self) -> Dict[str, Path]:
        """
        The paths to the data directories for each `process_run` in the processes
        chain specified by `self.branch_spec`, including only those which exist.
        """
        return self._paths

    @property
    def paths_exists(self) -> Dict[str, Path]:
        """
        Like `paths`, but existence not required.
        """
        return self._paths_exists

    def set_partition(self, process_name: Optional[str] = None, **kwargs):
        """Get new DataBranch with recursively updated `partition`"""
        raise NotImplementedError("This method should be implemented by `DataBranch` subclasses")

    def get_temp_meta_path(self: "DataBranch", process_name: str):
        return self[process_name].path / self.schema.__class__.TEMP_METADATA_FILENAME

    def __getitem__(self, process_name: str) -> ProcessRun:
        if process_name not in self._process_runs:
            process = self.branch_spec[process_name].process if process_name != "root" else "root"
            if process_name in ("root", None):
                process_name = "root"
                process = "root"
            self._process_runs[process_name] = ProcessRun(self, process_name, process)
        return self._process_runs[process_name]

    def _get_meta(self, process_name):
        raise NotImplementedError("This method should be implemented by `DataBranch` subclasses")

    @staticmethod
    def _combine_datasets(
        root: Union[str, Path],
        metadata: Optional[Union[str, Path]] = None,
        input_paths: Optional[List[Union[str, Path]]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")

    def _apply_data_ops(self, process_name: str, df: Optional[pd.DataFrame] = None):
        """
        Apply subset and filter operations to a dataframe where the operations
        are derived from the `branch_spec`, consecutively applying data operations,
        beginning with those corresponding to the first process, and ending
        with those corresponding to the process specified by `process_name`
        Args:
            process_name:

        Returns:

        """
        subset_list = self.branch_spec.get_subset_list(process_name)
        filter_list = self.branch_spec.get_filter_list(process_name)
        if df is None:
            self.set_meta(None)
            df = self.meta.copy()
        for (subset, filter_) in zip(subset_list, filter_list):
            for column, val in subset.items():
                df = self._do_subset(df, column, val)
            for column, val in filter_.items():
                df = self._do_filter(df, column, val)
        return df

    @staticmethod
    def _do_subset(df: pd.DataFrame, column: str, val: Any) -> pd.DataFrame:
        prev_df = df.copy()
        if isinstance(val, (list, set)):
            df = df[df[column].isin(val)]
        else:
            df = df[df[column] == val]
        if len(df) == len(prev_df):
            logging.warning(f"Subset didn't change num of rows and may be unnecessary - column: {column}, val: {val}")
        elif len(df) == 0:
            raise BadSubset(column, val)
        return df

    @staticmethod
    def _do_filter(df: pd.DataFrame, column: str, val: Any) -> pd.DataFrame:
        prev_df = df.copy()
        if isinstance(val, (list, set)):
            df = df[~df[column].isin(val)]
        else:
            df = df[df[column] != val]
        if len(df) == len(prev_df):
            logging.warning(f"Filter didn't change num of rows and may be unnecessary - column: {column}, val: {val}")
        elif len(df) == 0:
            raise BadFilter(column, val)
        return df

    def _map_file_io(self) -> Tuple[Dict[str, IOCache], Dict[str, IOCache]]:
        """
        Builds a lookup of lazy loading caches for file readers and writers,
        which have implicit access to paths, methods, and kwargs for each file.
        Returns:
            {reader, writer}_map:
                Key: file_alias (e.g. "rna")
                Value: IOCache (see class definition)
        """
        file_map = Tree(self.schema.__class__.FILE_MAP)
        reader_kwargs_map = file_map.apply_leaves(lambda x: dict()).dict
        writer_kwargs_map = file_map.apply_leaves(lambda x: dict()).dict
        reader_method_map = file_map.apply_leaves(self._map_default_reader_methods).dict
        writer_method_map = file_map.apply_leaves(self._map_default_writer_methods).dict
        update_recursive(reader_kwargs_map, self.READER_KWARGS_MAP, inplace=True)
        update_recursive(writer_kwargs_map, self.WRITER_KWARGS_MAP, inplace=True)
        update_recursive(reader_method_map.update, self.READER_MAP, inplace=True)
        update_recursive(writer_method_map.update, self.WRITER_MAP, inplace=True)
        reader_map, writer_map = dict(), dict()
        for process_name, file_dict in file_map.dict.items():
            if process_name in self.branch_spec.process_order:
                reader_method_dict = reader_method_map[process_name]
                writer_method_dict = writer_method_map[process_name]
                reader_kwargs_dict = reader_kwargs_map[process_name]
                writer_kwargs_dict = writer_kwargs_map[process_name]
                reader_map[process_name] = IOCache(
                    file_dict, reader_method_dict, reader_kwargs_dict, self._paths_exists
                )
                writer_map[process_name] = IOCache(
                    file_dict, writer_method_dict, writer_kwargs_dict, self._paths_exists
                )
        return reader_map, writer_map

    def _map_default_reader_methods(self, filename: str) -> Optional[Callable]:
        """
        Matches default `reader` methods to filenames from
        `self.READER_METHODS` based on file extensions, where the `reader`
        method name is punctuated by `_` rather than `.`
        Example:
            filename: `file.tsv.gz`
            method name: `tsv_gz`
        Args:
            filename: name of file to which a `reader` is to be assigned

        Returns:
            reader: method used to read data from `filename`
        """
        ext = "_".join(filename.split(".")[1:])
        if hasattr(self.READER_METHODS, ext):
            return getattr(self.READER_METHODS, ext)
        else:
            return None

    def _map_default_writer_methods(self, filename: str) -> Optional[Callable]:
        """
        Matches default `writer` methods to filenames from
        `self.WRITER_METHODS` based on file extensions, where the `writer`
        method name is punctuated by `_` rather than `.`
        Example:
            filename: `file.tsv.gz`
            method name: `tsv_gz`
        Args:
            filename: name of file to which a `writer` is to be assigned

        Returns:
            writer: method used to write data to `filename`
        """
        ext = "_".join(filename.split(".")[1:])
        if hasattr(self.WRITER_METHODS, ext):
            return getattr(self.WRITER_METHODS, ext, None)
        else:
            return None

    def _get_copy_base_kwargs(self):
        return {k: getattr(self, v) for k, v in self._COPY_KWARGS.items()}

    @staticmethod
    def _init_spec(branch_spec: Optional[Union[list, BranchSpec]]) -> BranchSpec:
        if branch_spec is None:
            branch_spec = list()
        if not isinstance(branch_spec, BranchSpec):
            branch_spec = BranchSpec(branch_spec)
        return branch_spec

    def _check_root_meta_match(self, root_other: Path):
        root_into_empty = (root_other / "meta.tsv").exists()
        if not root_into_empty:
            pd_kwargs = {"sep": "\t", "index_col": 0}
            meta = pd.read_csv(self["root"].path_map["meta"], **pd_kwargs)
            # TODO: hardcoded filename
            meta_other = pd.read_csv(root_other / "meta.tsv", **pd_kwargs)
            if not meta.equals(meta_other):
                raise ValueError("Cannot merge branches with two different metadata files at root")
