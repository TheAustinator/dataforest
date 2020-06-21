from typing import Callable, Dict, Optional, Set, Tuple, Union

from pathlib import Path

from dataforest.utils.utils import node_lineage_lookup


class ProcessSchema:
    """
    Descriptor base class for a hierarchical system of processes. Child classes
    are to specify the process schema by overloading the class attributes, and
    needn't add anything else.

    Class Attributes:
        FILE_MAP: mapping of `process_name`->`file_alias`->`filename`
            where the `file_alias` is the name by which the file will be
            referenced in the `DataForest`.
            Examples:
                {"process_1":
                   {"output_1": "arbitrary_filename.tsv.gz",
                    ...},
                 ...}

        PARAM_NAMES: lookup of param names for `process_names`
            Examples:
                {"process_1": {"alpha", "bias", ...},
                 ...}

        PROCESS_HIERARCHY: a nested `dict` which represents the process
            hierarchy. Sibling nodes which have the same child processes
            are represented as `tuple`s, and a level in which all sibling nodes
            are leaves is represented as a `set`.
            Examples:
                process_1
                └── process_2
                    ├── process_3_v1
                    │   ├── process_4
                    │   └── process_5
                    ├── process_3_v1
                    │   ├── process_4
                    │   └── process_5
                    └── auxiliary_process
                format:
                    {"process_1: {
                       "process_2": {
                         (process_3_v1, process_3_v2): {process_4, process_5},
                         },
                       "auxiliary_process": None,
                       },
                    }

        PROCESS_NAMES: names of processes to be used as keys throughout class

        ROOT_DIR: directory which contains top level `process_name` directory

        SUBSET_PROXIES: Proxies for non-equality based subsetting of DataFrame.
            The keys are proxy names, and the values are tuples that contain
            the operator which applies the subeset and the column name to which
            the subset is applies
            Examples: {"max_size": (operators.le, "size")

    Attributes:
        param_names: copy of `PARAM_NAMES` unless specified
        process_hierarchy: copy of `PROCESS_HIERARCHY` unless specified
        process_precursors: lookup of sequence of upstream processes for
            `process_name`s
            Example:
                {
                   'process_1': [],
                   'process_1': ['process_1'],
                   'process_3_v1': ['process_1', 'process_2'],
                   ...
                }
        root_dir: copy of ROOT_DIR
        subset_proxies:
    """

    FILE_MAP: Dict[str, Dict[str, str]] = dict()
    PARAM_NAMES: Dict[str, Set[str]] = dict()
    PROCESS_HIERARCHY: dict = dict()
    PROCESS_NAMES: Set[str] = set()
    SUBSET_PROXIES: Dict[str, Tuple[Callable, str]] = dict()

    def __init__(
        self,
        param_names: Optional[Union[str, Path, Union[Dict[str, Set[str]]]]] = None,
        process_hierarchy: Optional[Union[str, Path, dict]] = None,
        root_dir: Optional[Union[str, Path]] = None,
        subset_proxies: Optional[Dict[str, Tuple[Callable, str]]] = None,
        file_map: Optional[Union[Dict[str, Dict[str, str]], str, Path]] = None,
        process_names: Optional[Union[str, Path, Set[str]]] = None,
    ):
        self.param_names = (
            param_names if param_names is not None else self.PARAM_NAMES.copy()
        )
        self.process_hierarchy = (
            process_hierarchy
            if process_hierarchy is not None
            else self.PROCESS_HIERARCHY.copy()
        )
        self.process_precursors = node_lineage_lookup(self.PROCESS_HIERARCHY)
        self.subset_proxies = (
            subset_proxies if subset_proxies is not None else self.SUBSET_PROXIES.copy()
        )
        self.file_map = file_map if file_map is not None else self.FILE_MAP.copy()
        self.process_names = (
            process_names if process_names is not None else self.PROCESS_NAMES.copy()
        )
        if root_dir is not None:
            self.root_dir = root_dir
