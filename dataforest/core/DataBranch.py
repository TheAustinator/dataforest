import logging
from copy import deepcopy
from typing import Callable, Dict, Optional, Type, Union, List, Tuple, Any, Iterable, Literal

import pandas as pd
from pathlib import Path

from typeguard import typechecked

from dataforest.core.DataBase import DataBase
from dataforest.structures.cache.PathCache import PathCache
from dataforest.structures.cache.IOCache import IOCache
from dataforest.core.ProcessRun import ProcessRun
from dataforest.core.BranchSpec import BranchSpec
from dataforest.filesystem.tree.Tree import Tree

from dataforest.processes.core.ProcessMethods import ProcessMethods
from dataforest.core.schema.ProcessSchema import ProcessSchema
from dataforest.filesystem.io import ReaderMethods
from dataforest.filesystem.io.WriterMethods import WriterMethods
from dataforest.utils.exceptions import BadSubset, BadFilter
from dataforest.utils.utils import update_recursive, label_df_partitions
from dataforest.utils.warnings import manual_data_op_warning

Container = (set, list, tuple)


class DataBranch(DataBase):
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
            and `plot_kwargs`.

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

    PROCESS_METHODS: Type = ProcessMethods
    SCHEMA_CLASS: Type = ProcessSchema
    READER_METHODS: Type = ReaderMethods
    WRITER_METHODS: Type = WriterMethods
    DATA_FILE_ALIASES: set = set()
    READER_MAP: dict = dict()
    WRITER_MAP: dict = dict()
    READER_KWARGS_MAP: dict = dict()
    WRITER_KWARGS_MAP: dict = dict()
    _METADATA_NAME: dict = NotImplementedError("Should be implemented by superclass")
    _COPY_KWARGS: dict = {
        "root": "root",
        "branch_spec": "spec",
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
        super().__init__()
        self._meta = None
        self._unversioned = False
        self._current_process = current_process
        self._remote_root = remote_root
        self.root = Path(root)
        self.spec = self._init_spec(branch_spec)
        self.verbose = verbose
        self.logger = logging.getLogger(self.__class__.__name__)
        self.process = self.PROCESS_METHODS(self, self.spec)
        self.schema = self.SCHEMA_CLASS()
        self._paths_exists = PathCache(self.root, self.spec, exists_req=True)
        self._paths = self._paths_exists.get_shared_memory_view(exist_req=False)
        self._process_runs = dict()
        self._f = None
        self._w = None
        self._op_dict = {"subset": self._do_subset, "filter": self._do_filter}

    @property
    def current_process(self):
        """
        The name of the process from `branch_spec`, which attributes like `meta` and
        `rna` correspond to. For example, these data may be e.g. subset or
        directly modified as a result of some process. Use `goto_process` to
        change this property.
        """
        if not self._current_process:
            return "root"
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

    @property
    def f(self) -> Dict[str, IOCache]:
        """read lookup for files"""
        if self._f is None:
            self._f, self._w = self._map_file_io()
        return self._f

    @property
    def w(self) -> Dict[str, IOCache]:
        """write lookup for files"""
        if self._w is None:
            self._f, self._w = self._map_file_io()
        return self._w

    @property
    def unversioned(self):
        return self._unversioned

    @property
    def meta(self) -> pd.DataFrame:
        """
        Interface for cell sample_metadata, which is derived from the sample
        sample_metadata and the scrnaseq experimental data. Available UMAP embeddings
        and cluster identifiers will be included, and the data will be subset,
        filtered, and partitioned based on the specifications in `self.spec`.
        Primarily for this reason, this is the preferred interface to sample_metadata
        over direct file access.
        """
        if self._meta is None:
            self._meta = self._get_meta(self.current_process)
        return self._meta

    def add_metadata(self, df: pd.DataFrame, save=False):
        """
        Merge additional df of metadata with existing on index and optionally
        save at current process node, overwriting `meta.tsv`.
        """
        df_len = len(df)
        df = self.meta.merge(df, how="left", left_index=True, right_index=True)
        if len(df) < df_len:
            logging.warning(f"`merge_metadata` reduced len of input df from {df_len} to {len(df)}")
        if save:
            path = self[self.current_process].path / "meta.tsv"
            df.to_csv(path, sep="\t")
        self.set_meta(df)

    def subset(
        self, subset_dict: Optional[Dict[str, Any]] = None, indices: Optional[Union[pd.Series, List, Tuple]] = None,
    ):
        return self._apply_manual_data_op("subset", subset_dict, indices)

    def filter(
        self, filter_dict: Optional[Dict[str, Any]] = None, indices: Optional[Union[pd.Series, List, Tuple]] = None,
    ):
        return self._apply_manual_data_op("filter", filter_dict, indices)

    def goto_process(self, process_name: str) -> "DataBranch":
        """
        Updates the state of the `DataBranch` object to reflect the
        `ProcessRun` of `process_name` by:
            - clearing sample_metadata cache so that it can be recalculated to include
                only the sample_metadata up to and including that for `process_name`
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
            self.clear_data(path_map_changes)
        return self

    def groupby(self, by: Union[str, list, set, tuple], **kwargs) -> Tuple[str, "DataBranch"]:
        """
        Operates like a pandas group_labels, but does not return a GroupBy object,
        and yields (name, DataBranch), where each DataBranch is subset according to `by`,
        which corresponds to columns of `self.meta`.
        This is useful for batching analysis across various conditions, where
        each run requires an DataBranch.
        Args:
            by: variables over which to group (like pandas)
            **kwargs: for pandas group_labels on `self.meta`

        Yields:
            name: values for DataBranch `subset` according to keys specified in `by`
            branch: new DataBranch which inherits `self.spec` with additional `subset`s
                from `by`
        """
        if isinstance(by, (tuple, set)):
            by = list(by)
        for (name, df) in self.meta.groupby(by, **kwargs):
            branch = self.copy()
            branch.set_meta(df)
            yield name, branch

    def clear_data(self, attrs: Optional[Iterable[str]] = None, all_data: bool = False):
        if not (attrs != None or all_data):
            raise ValueError("Must provide `args` or `all_data`")
        attrs = self.DATA_FILE_ALIASES if all_data else attrs
        for attr_name in attrs:
            data_attr = f"_{attr_name}"
            setattr(self, data_attr, None)

    def is_process_plots_exist(self, process_name: str) -> bool:
        return self[process_name].plots_path.exists()

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

        # TODO: if root sample_metadata, check that it's the same
        # TODO: goto each process in chain and get file map
        # TODO: update `done` to use something besides logs
        # TODO: copy file map, evaluate all elements for `done` processes
        # TODO: reconcile and register each process in run_catalogue
        # TODO: replace roots with `remote_root`
        # TODO: copy files

        raise NotImplementedError()

    def copy(self, **kwargs):
        inst = self.__class__(**kwargs)
        # TODO: not efficient to make them all store this, but neet to persistence
        inst.set_meta(self.meta)
        return inst

    def copy_legacy(self, **kwargs) -> "DataBranch":
        base_kwargs = self._get_copy_base_kwargs()
        kwargs = {**base_kwargs, **kwargs}
        kwargs = {k: deepcopy(v) for k, v in kwargs.items()}
        return self.__class__(**kwargs)

    def set_meta(self, df: Optional[pd.DataFrame]):
        self._meta = df.copy()

    @property
    def paths(self) -> Dict[str, Path]:
        """
        The paths to the data directories for each `process_run` in the processes
        chain specified by `self.spec`, including only those which exist.
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
            process_name = "root" if process_name is None else process_name
            process = self.spec[process_name].process if process_name != "root" else "root"
            self._process_runs[process_name] = ProcessRun(self, process_name, process)
        return self._process_runs[process_name]

    @property
    def current_path(self) -> str:
        """
        The paths at current `process_run`
        """
        return self._paths[self._current_process]

    @staticmethod
    def _combine_datasets(
        root_dir: Union[str, Path],
        metadata: Optional[Union[str, Path]] = None,
        input_paths: Optional[List[Union[str, Path]]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")

    def _get_meta(self, process_name):
        raise NotImplementedError("This method should be implemented by `DataBranch` subclasses")

    def _apply_manual_data_op(
        self,
        operation: Literal["subset", "filter"],
        data_op_dict: Dict[str, Any],
        indices: Optional[Union[pd.Series, List, Tuple]] = None,
    ):
        branch = self.copy()
        if bool(data_op_dict) == bool(indices is not None):
            raise ValueError("Must specify col and vals together or indices alone")
        logging.warning(manual_data_op_warning)
        if data_op_dict:
            meta = branch._apply_data_op(branch.meta, data_op_dict, operation)
            branch.set_meta(meta)
        else:
            meta = branch.meta.loc[indices]
            branch.set_meta(meta)
        return branch

    def _apply_data_op(self, df: pd.DataFrame, operation_dict: Dict[str, Any], operation: Literal["subset", "filter"]):
        df = df.copy()
        op = self._op_dict[operation]
        for col, val in operation_dict.items():
            if "_MULTI_" in col:
                df = op(df, val)
            elif val is not None:
                df = op(df, {col: val})
        return df

    def _apply_data_ops_auto(self, process_name: str, df: Optional[pd.DataFrame] = None):
        """
        Apply subset and filter operations to a dataframe where the operations
        are derived from the `branch_spec`, consecutively applying data operations,
        beginning with those corresponding to the first process, and ending
        with those corresponding to the process specified by `process_name`
        Args:
            process_name:

        Returns:

        """
        subset_list = self.spec.get_subset_list(process_name)
        filter_list = self.spec.get_filter_list(process_name)
        if df is None:
            self.set_meta(None)
        for (subset, filter_) in zip(subset_list, filter_list):
            df = self._apply_data_op(df, subset, "subset")
            df = self._apply_data_op(df, filter_, "filter")
        df.replace(" ", "_", regex=True, inplace=True)
        partitions_list = self.spec.get_partition_list(process_name)
        partitions = set().union(*partitions_list)
        if partitions:
            df = label_df_partitions(df, partitions, encodings=True)
        return df

    @classmethod
    def _do_subset(cls, df: pd.DataFrame, subset_dict: Dict[str, Any]) -> pd.DataFrame:
        df_selector = cls._get_df_selector(df, subset_dict)
        df = df.loc[df_selector]
        if len(df) == 0:
            raise BadSubset(subset_dict)
        return df

    @classmethod
    def _do_filter(cls, df: pd.DataFrame, filter_dict: Dict[str, Any]) -> pd.DataFrame:
        df_selector = cls._get_df_selector(df, filter_dict)
        df = df.loc[~df_selector]
        if len(df) == 0:
            raise BadFilter(filter_dict)
        return df

    @staticmethod
    def _get_df_selector(df: pd.DataFrame, op_dict: Dict[str, Any]) -> pd.Series:
        def _check_row_eq(row: pd.Series) -> bool:
            for key, val in row.iteritems():
                op_val = op_dict[key]
                if isinstance(val, Container) and isinstance(op_val, Container):
                    if not set(val).union(op_val):
                        return False
                elif isinstance(val, Container):
                    if op_val not in val:
                        return False
                elif isinstance(op_val, Container):
                    if val not in op_val:
                        return False
                else:
                    if op_val != val:
                        return False
            return True

        df_selector = pd.DataFrame(pd.DataFrame(df[list(op_dict)]).apply(_check_row_eq, axis=1)).all(axis=1)
        return df_selector

    def _map_file_io(self) -> Tuple[Dict[str, IOCache], Dict[str, IOCache]]:
        """
        Builds a lookup of lazy loading caches for file readers and writers,
        which have implicit access to paths, methods, and plot_kwargs for each file.
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
            if process_name in self.spec.process_order:
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
            try:
                meta_other = pd.read_csv(root_other / "meta.tsv", **pd_kwargs)
            except Exception as e:
                raise e
            if not meta.equals(meta_other):
                raise ValueError("Cannot merge branches with two different sample_metadata files at root")
