import logging
from copy import deepcopy
from typing import Callable, Dict, Optional, Type, Union, List, Iterable, Tuple, Any

import pandas as pd
from pathlib import Path

from dataforest.structures.cache.PathCache import PathCache
from dataforest.structures.cache.IOCache import IOCache
from dataforest.core.ProcessRun import ProcessRun
from dataforest.core.Spec import Spec
from dataforest.filesystem.tree.Tree import Tree

# from dataforest.hyperparams.HyperparameterMethods import HyperparameterMethods
from dataforest.processes.core.BatchMethods import BatchMethods
from dataforest.plot.PlotMethods import PlotMethods
from dataforest.processes.core.ProcessMethods import ProcessMethods
from dataforest.core.schema.ProcessSchema import ProcessSchema
from dataforest.filesystem.io import ReaderMethods
from dataforest.filesystem.io.WriterMethods import WriterMethods
from dataforest.utils.exceptions import BadSubset, BadFilter
from dataforest.utils.loaders.update_config import update_config
from dataforest.utils.utils import update_recursive


class DataForest:
    """
    Core interface for package, which is used to access hierarchical data
    output by chains of processes, whose `process_run`s can be described by the
    processes `params` and input data. At the root level processes, the input data
    is specified by `params`, and in subsequent processes, the input data is
    specified by `subset`s, `filter`s, and `partition`s derived from that initial
    data. `subset`s specify criteria for inclusion, `filter`s specify criteria
    for exclusion, and `partition`s specify criteria for partitioning in
    comparative analyses.
    >>> root_dir = "/data/root:dataset_1"
    >>> spec = {
    >>>     "process_1":
    >>>         {
    >>>             "epsilon": .1,    # param
    >>>             "batch": {3, 4, 5},    # subset
    >>>             "filter": {"month": {"march", "april"}}    # filter
    >>>         },
    >>>     "process_2": {
    >>>             "alpha": 0.05,    # param
    >>>             "partition": "treatment",    # partition
    >>>         }
    >>> }
    >>> forest = DataForest(root_dir, spec)

    Class Attributes:
        SCHEMA_CLASS: Point to a subclass of `ProcessSchema`

        SPEC_CLASS: Points to a subclass of `Spec`

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
        _paths (PathCache): list of all existing paths through hierarchical
            processes filesystem as dicts in `Spec` format.
        logger:
        schema:
        spec:
        _io_map:
        _reader_kwargs_map:
        _reader_method_map:
        _writer_kwargs_map:
        _writer_method_map:n
    """

    PLOT_METHODS: Type = PlotMethods
    PROCESS_METHODS: Type = ProcessMethods
    SCHEMA_CLASS: Type = ProcessSchema
    SPEC_CLASS: Type = Spec
    READER_METHODS: Type = ReaderMethods
    WRITER_METHODS: Type = WriterMethods
    BATCH_METHODS: Type = BatchMethods
    # HYPERPARAMETER_METHODS: Type = HyperparameterMethods
    READER_MAP: dict = dict()
    WRITER_MAP: dict = dict()
    READER_KWARGS_MAP: dict = dict()
    WRITER_KWARGS_MAP: dict = dict()
    _METADATA_NAME: dict = NotImplementedError("Should be implemented by superclass")
    _COPY_KWARGS: dict = {
        "root_dir": "root_dir",
        "spec": "spec",
        "verbose": "verbose",
        "current_process": "current_process",
    }
    _DEFAULT_CONFIG = Path(__file__).parent / "config/default_config.yaml"

    def __init__(
        self,
        root_dir: Union[str, Path],
        spec: Optional[List[dict]] = None,
        verbose: bool = False,
        config: Optional[Union[dict, str, Path]] = None,
        current_process: Optional[str] = None,
    ):
        if config is not None:
            update_config(config)
        else:
            update_config(self._DEFAULT_CONFIG)
        self._meta = None
        self._unversioned = False
        self._current_process = current_process
        self.root_dir = Path(root_dir)
        self.spec = self._init_spec(spec)
        self.verbose = verbose
        # TODO: log file operations if verbose
        self.logger = logging.getLogger(self.__class__.__name__)
        self.plot = self.PLOT_METHODS(self)
        self.process = self.PROCESS_METHODS(self, self.spec)
        # self.hyper = HyperparameterMethods(self)
        self.schema = self.SCHEMA_CLASS()
        self._paths = PathCache(self.root_dir, self.spec)
        self._process_runs = dict()
        self._file_tree = Tree(self.schema.__class__.FILE_MAP)
        self._reader_kwargs_map = self._file_tree.apply_leaves(lambda x: dict()).dict
        self._writer_kwargs_map = self._file_tree.apply_leaves(lambda x: dict()).dict
        self._reader_method_map = self._file_tree.apply_leaves(self._map_default_reader_methods).dict
        self._writer_method_map = self._file_tree.apply_leaves(self._map_default_writer_methods).dict
        update_recursive(self._reader_kwargs_map, self.READER_KWARGS_MAP, inplace=True)
        update_recursive(self._writer_kwargs_map, self.WRITER_KWARGS_MAP, inplace=True)
        update_recursive(self._reader_method_map.update, self.READER_MAP, inplace=True)
        update_recursive(self._writer_method_map.update, self.WRITER_MAP, inplace=True)
        self.f, self.w = self._map_file_io()

    @property
    def current_process(self):
        """
        The name of the process from `spec`, which attributes like `meta` and
        `rna` correspond to. For example, these data may be e.g. subset or
        directly modified as a result of some process. Use `goto_process` to
        change this property.
        """
        return self._current_process

    @classmethod
    def load(cls, root_dir, **kwargs):
        return cls(root_dir, **kwargs)

    @classmethod
    def from_input_dirs(
        cls,
        root_dir: Union[str, Path],
        input_paths: Optional[Union[str, Path, Iterable[Union[str, Path]]]] = None,
        mode: Optional[str] = None,
        **kwargs,
    ) -> "DataForest":
        """
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataForest is instantiated.
        The input directories are specified either via
        Args:
            input_paths: list of input data directories
            root_dir: root directory to deposit combined files
        """
        if not isinstance(input_paths, (list, tuple)):
            input_paths = [input_paths]
        additional_kwargs = cls._combine_datasets(root_dir, input_paths=input_paths, mode=mode)
        kwargs = {**additional_kwargs, **kwargs}
        return cls(root_dir, **kwargs)

    @classmethod
    def from_metadata(
        cls, root_dir: Union[str, Path], metadata: Optional[pd.DataFrame] = None, **kwargs,
    ) -> "DataForest":
        """
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataForest is instantiated.
        The input directories are specified either via
        Args:
            root_dir: root directory to deposit combined files
            metadata: path to metadata, where each row corresponds to a
                single dataset from `input_dirs`. The only column which must
                be present is `path`, which must be matched to the elements of
                `input_dirs`
        """
        additional_kwargs = cls._combine_datasets(root_dir, metadata=metadata)
        kwargs = {**additional_kwargs, **kwargs}
        return cls(root_dir, **kwargs)

    def goto_process(self, process_name: str) -> "DataForest":
        """
        There can be multiple specifications for `subset`, `filter`, and
        `partition` under different `process_name`s in `self.spec`, and also
        at the root level (at the base level of `self.spec`). When running a
        particular processes, specifications for that processes may occur
        both/either at `process_name` and/or at root. To account for either
        case, we want to aggregate these.

        This method establishes uniformity of `subset`, `filter`, and
        `partition` in `self.spec` between root level and `process_name`.
        First, the entries for `process_name` are updated from the root level,
        then the root level is updated from the processes name. If there are no
        conflicting keys between root and `process_name`, this order doesn't
        matter, but in the case of conflicts, the value at root will overwrite
        that at `process_name`.

        Note that `params` are left out since they are specific to
        `process_name`, and not used to slice or modify data in the `DataForest`
        Args:
            process_name: processes name under `self.spec` with which to
                establish uniformity of `self.spec`.

        Returns:
            A spec with an updated DataForest which has the aggregate s
        """

        # TODO: CORE update -- duplication still required, but maybe we can use the _filter_subset method

        if self.unversioned:
            self.logger.warning("Calling `at` on unversioned `DataForest`")
        self._current_process = process_name
        self._apply_data_operations(process_name)
        return self

    def copy(self):
        inst = deepcopy(self)
        inst.reset_meta()
        return inst

    def copy_legacy(self, **kwargs) -> "DataForest":
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
        chain specified by `self.spec`
        """
        return self._paths

    def set_partition(self, process_name: Optional[str] = None, **kwargs):
        """Get new DataForest with recursively updated `partition`"""
        raise NotImplementedError("This method should be implemented by `DataForest` subclasses")

    def get_temp_meta_path(self: "DataForest", process_name: str):
        return self[process_name].path / self.schema.__class__.TEMP_METADATA_FILENAME

    def __getitem__(self, process_name: str) -> ProcessRun:
        if process_name not in self._process_runs:
            process = self.spec[process_name].process
            self._process_runs[process_name] = ProcessRun(self, process_name, process)
        return self._process_runs[process_name]

    @staticmethod
    def _combine_datasets(
        root_dir: Union[str, Path],
        metadata: Optional[Union[str, Path]] = None,
        input_paths: Optional[List[Union[str, Path]]] = None,
        mode: Optional[str] = None,
    ):
        raise NotImplementedError("Must be implemented by subclass")

    def _apply_data_operations(self, process_name: str):
        """
        Apply subset and filter operations to a dataframe where the operations
        are derived from the `spec`, consecutively applying data operations,
        beginning with those corresponding to the first process, and ending
        with those corresponding to the process specified by `process_name`
        Args:
            process_name:

        Returns:

        """
        subset_list = self.spec.get_subset_list(process_name)
        filter_list = self.spec.get_filter_list(process_name)
        self.set_meta(None)
        meta = self.meta.copy()
        for (subset, filter_) in zip(subset_list, filter_list):
            for column, val in subset.items():
                meta = self._do_subset(meta, column, val)
            for column, val in filter_.items():
                meta = self._do_filter(meta, column, val)
        self.set_meta(meta)

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
        reader_map = dict()
        writer_map = dict()
        for process_name, file_dict in self.schema.__class__.FILE_MAP.items():
            if process_name in self.spec.process_order:
                reader_method_dict = self._reader_method_map[process_name]
                writer_method_dict = self._writer_method_map[process_name]
                reader_kwargs_dict = self._reader_kwargs_map[process_name]
                writer_kwargs_dict = self._writer_kwargs_map[process_name]
                reader_map[process_name] = IOCache(file_dict, reader_method_dict, reader_kwargs_dict, self._paths)
                writer_map[process_name] = IOCache(file_dict, writer_method_dict, writer_kwargs_dict, self._paths)
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

    def _init_spec(self, spec: Optional[Union[dict, Spec]]) -> Spec:
        if spec is None:
            spec = dict()
        if not isinstance(spec, Spec):
            spec = self.SPEC_CLASS(spec)
        return spec
