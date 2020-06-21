import logging
from copy import deepcopy
from typing import Callable, Dict, Optional, Type, Union, Any

import pandas as pd
from pathlib import Path

from dataforest.core.DataMap import DataMap
from dataforest.core.ProcessRun import ProcessRun
from dataforest.core.Spec import Spec
from dataforest.filesystem.DataTree import DataTree
from dataforest.filesystem.FileIO import FileIO
from dataforest.filesystem.Tree import Tree
from dataforest.hyperparams.HyperparameterMethods import HyperparameterMethods
from dataforest.templates.BatchMethods import BatchMethods
from dataforest.templates.PlotMethods import PlotMethods
from dataforest.templates.ProcessSchema import ProcessSchema
from dataforest.templates.ReaderMethods import ReaderMethods
from dataforest.templates.WriterMethods import WriterMethods
from dataforest.utils.utils import update_recursive


class DataForest:
    """
    Core interface for package, which is used to access hierarchical data
    output by chains of processes, whose `process_run`s can be described by the
    process `params` and input data. At the root level process, the input data
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
        data_map (DataMap): list of all existing paths through hierarchical
            process filesystem as dicts in `Spec` format.
        logger:
        schema:
        spec:
        _io_map:
        _reader_kwargs_map:
        _reader_map:
        _writer_kwargs_map:
        _writer_map:
    """

    SCHEMA_CLASS: Type = ProcessSchema
    SPEC_CLASS: Type = Spec
    READER_METHODS: Type = ReaderMethods
    WRITER_METHODS: Type = WriterMethods
    PLOT_METHODS: Type = PlotMethods
    BATCH_METHODS: Type = BatchMethods
    HYPERPARAMETER_METHODS: Type = HyperparameterMethods
    READER_MAP: dict = dict()
    WRITER_MAP: dict = dict()
    READER_KWARGS_MAP: dict = dict()
    WRITER_KWARGS_MAP: dict = dict()
    METADATA_NAME: dict = NotImplementedError("Should be implemented by superclass")
    COPY_KWARGS: dict = {
        "root_dir": "root_dir",
        "spec_dict": "spec",
        "verbose": "verbose",
    }

    def __init__(
            self,
            root_dir: Union[str, Path],
            spec_dict: Optional[Dict[str, Dict[str, Any]]] = None,
            verbose: bool = False,
            config_dir: Optional[Union[str, Path]] = None,
    ):
        self._meta = None
        self._unversioned = False
        self.root_dir = Path(root_dir)
        self.verbose = verbose
        # TODO log file operations if verbose
        self.logger = logging.getLogger(self.__class__.__name__)
        self._spec_warnings = set()
        self.plot = self.PLOT_METHODS(self)
        self.hyper = HyperparameterMethods(self)
        self.schema = self.SCHEMA_CLASS()
        self.data_map = DataMap(self.root_dir, self.schema)
        if spec_dict is None:
            spec_dict = dict()
        if isinstance(spec_dict, Spec):
            self.spec = spec_dict
        else:
            self.spec = self.SPEC_CLASS(self.data_map, self.schema, spec_dict)
        self._file_tree = Tree(self.SCHEMA_CLASS.FILE_MAP)
        self._reader_kwargs_map = self._file_tree.apply_leaves(lambda x: dict()).dict
        self._writer_kwargs_map = self._file_tree.apply_leaves(lambda x: dict()).dict
        self._reader_map = self._file_tree.apply_leaves(
            self._map_default_reader_methods
        ).dict
        self._writer_map = self._file_tree.apply_leaves(
            self._map_default_writer_methods
        ).dict
        update_recursive(self._reader_kwargs_map, self.READER_KWARGS_MAP, inplace=True)
        update_recursive(self._writer_kwargs_map, self.WRITER_KWARGS_MAP, inplace=True)
        update_recursive(self._reader_map.update, self.READER_MAP, inplace=True)
        update_recursive(self._writer_map.update, self.WRITER_MAP, inplace=True)
        self._io_map = self._map_file_io()
        self._map_file_data_properties()
        self._process_runs = dict()

    def at(self, process_name: str) -> "DataForest":
        """
        There can be multiple specifications for `subset`, `filter`, and
        `partition` under different `process_name`s in `self.spec`, and also
        at the root level (at the base level of `self.spec`). When running a
        particular process, specifications for that process may occur
        both/either at `process_name` and/or at root. To account for either
        case, we want to aggregate these.

        This method establishes uniformity of `subset`, `filter`, and
        `partition` in `self.spec` between root level and `process_name`.
        First, the entries for `process_name` are updated from the root level,
        then the root level is updated from the process name. If there are no
        conflicting keys between root and `process_name`, this order doesn't
        matter, but in the case of conflicts, the value at root will overwrite
        that at `process_name`.

        Note that `params` are left out since they are specific to
        `process_name`, and not used to slice or modify data in the `DataForest`
        Args:
            process_name: process name under `self.spec` with which to
                establish uniformity of `self.spec`.

        Returns:
            A spec with an updated DataForest which has the aggregate s
        """

        if self.unversioned:
            self.logger.warning("Calling `at` on unversioned `DataForest`")
        if process_name not in self.schema.PROCESS_NAMES:
            raise KeyError(
                f"Invalid process_name: {process_name}. Options: {self.schema.PROCESS_NAMES}"
            )

        spec = self.spec.copy()
        processes = self.schema.process_precursors[process_name] + [process_name]
        stationary_keys = list(self.schema.PROCESS_NAMES)
        # TODO: remove inner udpate?
        inner_update = {k: v for k, v in spec.items() if k not in stationary_keys}
        update_recursive(spec[process_name], inner_update, inplace=True)
        for name in processes:
            outer_update = dict()
            if "filter" in spec[name]:
                outer_update = {"filter": spec[name]["filter"]}
            stationary_keys = list(self.schema.param_names[name])  # + ['partition']
            subset_update = {
                k: v for k, v in spec[name].items() if k not in stationary_keys
            }
            outer_update = {**outer_update, **subset_update}
            update_recursive(spec, outer_update, inplace=True)
        meta = self._meta if self.unversioned else None
        inst = self.copy(spec_dict=dict(spec), meta=meta)
        return inst

    def copy(self, **kwargs) -> "DataForest":
        base_kwargs = self._get_copy_base_kwargs()
        kwargs = {**base_kwargs, **kwargs}
        kwargs = {k: deepcopy(v) for k, v in kwargs.items()}
        return self.__class__(**kwargs)

    @property
    def meta(self) -> pd.DataFrame:
        raise NotImplementedError("Should be implemented by subclass")

    @property
    def paths(self) -> Dict[str, Path]:
        """
        The paths to the data directories for each `process_run` in the process
        chain specified by `self.spec`
        """
        data_paths = {}
        for process, precursors in self.schema.process_precursors.items():
            path = self.root_dir
            process_chain = precursors + [process]
            for step in process_chain:
                if step not in self._process_subpaths:
                    if step not in self._spec_warnings:
                        self.logger.debug(
                            f"step {step} not specified in `Spec` and will be excluded from DataForest"
                        )
                        self._spec_warnings.add(step)
                    continue
                path /= "/".join((step, self._process_subpaths[step]))
            data_paths[process] = path
        return data_paths

    @property
    def process_spec(self) -> dict:
        """
        Portion of `Spec` which pertains to process params rather than input
        data specification
        """
        return {k: v for k, v in self.spec.items() if k in self.schema.PROCESS_NAMES}

    def set_partition(self, process_name: Optional[str] = None, **kwargs):
        """Get new DataForest with recursively updated `partition`"""
        raise NotImplementedError(
            "This method should be implemented by `DataForest` subclasses"
        )

    def get_subset(self, subset_dict: dict) -> "DataForest":
        """Get new DataForest with recursively updated `subset`"""
        return self._get_compartment_updated("subset", subset_dict)

    def get_filtered(self, filter_dict: dict) -> "DataForest":
        """Get new DataForest with recursively updated `filtered`"""
        return self._get_compartment_updated("filter", filter_dict)

    def _subset_filter(
            self,
            df: pd.DataFrame,
            spec: Spec,
            schema: ProcessSchema,
            process_name: Optional[str] = None,
    ) -> pd.DataFrame:
        unnecessary_keys = dict()
        unnecessary_filters = dict()
        # TODO: self._meta starts out as f_cell_metadata with joined and calculated values, then
        #   this properly filters and subsets it
        # TODO: this is run at the root level during init of DataForest.meta, but only run with process_names
        #   during `DataForest.at`
        prev_df = df.copy()
        subset_dict = spec.get_subset_dict(spec, schema, process_name)
        filter_dict = spec.get_filter_dict(spec, process_name)
        for key, val in subset_dict.items():
            # TODO: if process_name is None, do_subset should exclude process_names, otherwise,
            #   exclude params for that process
            subset = {key: val}
            df = self._do_subset(prev_df, subset)
            if len(df) == len(prev_df):
                unnecessary_keys.update(subset)
            prev_df = df
        for key, val in filter_dict.items():
            filter_ = {key: val}
            df = self._do_filter(prev_df, filter_)
            if len(df) == len(prev_df):
                unnecessary_filters.update(filter_)
            prev_df = df
        if unnecessary_filters:
            unnecessary_keys.update({"filter": unnecessary_filters})
        if unnecessary_keys:
            logging.warning(
                f"Some keys may be unecessary in process : {process_name}: {unnecessary_keys}"
            )
            # raise UnnecessaryKeysError(unnecessary_keys, process_name)
        if len(df) == 0:
            raise ValueError(
                f"subset and filter resulted in no rows. subset: {subset_dict} and filter: {filter_dict}. Please note "
                f"that any spaces in `DataForest.spec` dict should be converted to underscores!"
            )
        return df

    @staticmethod
    def _do_subset(df: pd.DataFrame, subset_dict: dict) -> pd.DataFrame:
        for key, val in subset_dict.items():
            if isinstance(val, (list, set)):
                df = df[df[key].isin(val)]
            else:
                df = df[df[key] == val]
        return df

    @staticmethod
    def _do_filter(df: pd.DataFrame, subset_dict: dict) -> pd.DataFrame:
        for key, val in subset_dict.items():
            if isinstance(val, (list, set)):
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key] != val]
        return df

    def _get_compartment_updated(
            self, compartment_name: str, update: dict
    ) -> "DataForest":
        spec_dict = update_recursive(
            self.spec.copy(), {compartment_name: update}, inplace=False
        )
        # spec = self.SPEC_CLASS(self.data_map, self.schema, spec_dict)
        return self.copy(spec_dict=spec_dict)

    @property
    def data_spec(self) -> dict:
        """
        Portion of `Spec` which pertains to data specification rather than
        process params
        """
        return {
            k: v for k, v in self.spec.items() if k not in self.schema.PROCESS_NAMES
        }

    def __getitem__(self, process_name: str) -> ProcessRun:
        if process_name not in self._process_runs:
            self._process_runs[process_name] = ProcessRun(self, process_name)
        return self._process_runs[process_name]

    @property
    def _process_subpaths(self) -> Dict[str, str]:
        """
        A lookup based on the process specifications in `self.spec` with
        keys as `process_name`s and values as `ForestQuery` strings
        corresponding to the `process_run`
        """
        subpaths = dict()
        for process_name, specs in self.process_spec.items():
            subpaths[process_name] = str(
                DataTree(specs, self.schema.param_names[process_name])
            )
        return subpaths

    def _map_file_io(self) -> Dict[str, FileIO]:
        """
        Assigns a `FilIO` to each file specified in `self.schema.FILE_MAP`.
        Each `FileIO` is assigned a `reader` and a `writer` based on
        `self.reader_map` and `self.writer_map`, respectively. `reader_kwargs`
        and `writer_kwargs` are extracted from `self.reader_kwargs_map` and
        `self.writer_kwargs_map` to provide custom keyword arguments for each
        file that will be passed to the `reader` and `writer` when called.
        The io_map is flat dictionary with keys as `file_aliases` from
        `self.schema.FILE_MAP` and corresponding `FileIO`s as values.
        Returns:
            io_map: lookup with `file_alias` keys and `FileIO` values
        """
        io_map = dict()
        for process_name, file_dict in self.schema.FILE_MAP.items():
            for file_alias, filename in file_dict.items():
                filepath = self.paths[process_name] / filename
                reader = self._reader_map[process_name][file_alias]
                writer = self._writer_map[process_name][file_alias]
                reader_kwargs = self._reader_kwargs_map[process_name][file_alias]
                writer_kwargs = self._writer_kwargs_map[process_name][file_alias]
                file_io = FileIO(filepath, reader, writer, reader_kwargs, writer_kwargs)
                io_map[file_alias] = file_io
        return io_map

    def _map_file_data_properties(self):
        """
        Meta-programming approach to add 3 attributes for each file alias.
        1. DataForest.f_{file_alias}: property which accesses reader method if data
            isn't already cached, otherwise, accesses cache
        2. DataForest.write_{file_alias}: writer method for given `file_alias`
        3. DataForest._cache_{file_alias}: stores cached data from `FileIO` after first
            read
        """
        for file_alias in self._io_map:
            file_data_attr = f"f_{file_alias}"
            file_data_write_attr = f"write_{file_alias}"
            file_data_cache = f"_cache_{file_alias}"
            setattr(
                self.__class__, file_data_attr, self._build_file_data_kernel(file_alias)
            )
            setattr(self, file_data_write_attr, self._io_map[file_alias].writer)
            setattr(self, file_data_cache, None)

    @staticmethod
    def _build_file_data_kernel(file_alias: str) -> property:
        """
        Kernel which creates a data access property for a given `file_alias`.
        The property first tries to retrieve cached data, and if there is none,
        it reads the data from the `reader` at `DataForest._io_map[file_alias]` and
        caches it.
        Args:
            file_alias:

        Returns:

        """

        def func(forest):
            file_data_cache = f"_cache_{file_alias}"
            if getattr(forest, file_data_cache) is None:
                file_io = forest._io_map[file_alias]
                setattr(forest, file_data_cache, file_io.read())
            return getattr(forest, file_data_cache)

        return property(func)

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
        return {k: getattr(self, v) for k, v in self.COPY_KWARGS.items()}
