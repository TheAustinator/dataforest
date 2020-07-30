from typing import Callable, Dict, Optional, Set, Tuple, Union, Iterable

from pathlib import Path

from dataforest.core.schema.MetaProcessSchema import MetaProcessSchema


class ProcessSchema(metaclass=MetaProcessSchema):
    """
    Descriptor base class for a hierarchical system of processes. Child classes
    are to specify the processes schema by overloading the class attributes, and
    needn't add anything else.

    Class Attributes:
        FILE_MAP: mapping of `process_name`->`file_alias`->`filename`
            where the `file_alias` is the name by which the file will be
            referenced in the `DataBranch`.
            Examples:
                {"process_1":
                   {"output_1": "arbitrary_filename.tsv.gz",
                    ...},
                 ...}

        PROCESS_HIERARCHY: a nested `dict` which represents the processes
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
        process_precursors: lookup of sequence of upstream processes for
            `process_name`s
            Example:
                {
                   'process_1': [],
                   'process_1': ['process_1'],
                   'process_3_v1': ['process_1', 'process_2'],
                   ...
                }
        root: copy of ROOT_DIR
        subset_proxies:
    """

    def __init__(
        self,
        root: Optional[Union[str, Path]] = None,
        subset_proxies: Optional[Dict[str, Tuple[Callable, str]]] = None,
        file_map: Optional[Union[Dict[str, Dict[str, str]], str, Path]] = None,
    ):
        # TODO: QUEUE allow these params to be passed to dataforest
        self.subset_proxies = subset_proxies if subset_proxies is not None else self.__class__.SUBSET_PROXIES.copy()
        self.file_map = file_map if file_map is not None else self.__class__.FILE_MAP.copy()
        if root is not None:
            self.root = root
