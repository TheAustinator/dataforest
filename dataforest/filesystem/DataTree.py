from copy import deepcopy
from typing import List, Optional, Tuple, Union

from dataforest.filesystem.Tree import Tree
from dataforest.utils.utils import update_recursive


class DataTree:
    """
    Data and process parameters specification for a given `process_run`.
    Uses `ForestQuery` language to translate between directory name strings
    which contain hierarchical information, and `dict`s. Contains all
    information necessary to replicate a `process_run` except code versioning,
    so long as it stays within the directories of predecessor `process_run`s.
    """

    UP = "-"
    DOWN = ":"
    AND = "+"
    FILTER = "!"
    PARTITION = "@"
    SPLIT = "/"
    UNVERSIONED = "?"
    OPERATORS = (UP, DOWN, AND, FILTER, PARTITION, UNVERSIONED)

    def __init__(self, input_dict: dict = None, params_names: Union[tuple, list] = ()):
        self.dict = {"subset": [], "filter": [], "partition": [], "params": Tree()}
        self.active_tree = None
        self.param_names = params_names
        self.unversioned = False
        if input_dict:
            self._merge(input_dict, params_names)

    @property
    def simple(self) -> dict:
        """
        A simplified dictionary representation which assumes only a one of each
        of `subset`, `filter`, and `partition` by removing the `list` wrapper
        around the `Tree`s for `filter` and `partition`, and combining the `Tree`s
        of `params` and `subset` at the base level of the dict. All `Tree` data
        structures are converted to `dict`.
        """
        simple = dict()
        simple.update(self.params.dict)
        if self.subset:
            if len(self.filter) > 1:
                raise ValueError(
                    f"Multiple subsets cannot be converted to `simple` format: {self.subset}"
                )
            simple.update(self.subset[0].dict)
        if self.filter:
            if len(self.filter) > 1:
                raise ValueError(
                    f"Multiple filters cannot be converted to `simple` format: {self.filter}"
                )
            simple.update({"filter": self.filter[0].dict})
        if self.partition:
            simple.update(
                {"partition": {k for part in self.partition for k in part.dict}}
            )
        return simple

    @property
    def subset(self) -> Tree:
        """
        Data specification for inclusion, where keys and values generally
        correspond to table column names and values, respectively.
        """
        return self.dict["subset"]

    @property
    def filter(self):
        """
        Data specification for exclusion, where keys and values generally
        correspond to table column names and values, respectively.
        """
        return self.dict["filter"]

    @property
    def partition(self):
        """
        Data specification for partitioning for comparative analysis. When
        instantiated with a `set`, the values in the `set` generally
        correspond to column names in a table, and a partition is formed for
        each combination that exists in the table. When instantiated with a
        `dict`, a the keys and values correspond to table column names and
        values, respectively. A partition is formed for each combination of
        values specified that exists in the table.
        """
        return self.dict["partition"]

    @property
    def params(self):
        """
        Process specification for the parameters fed to the `process_run`. Each
        key, value pair corresponds to a parameter name, value pair for the
        `process_run`.
        """
        return self.dict["params"]

    @classmethod
    def from_str(cls, str_: str, params_names: Union[list, set, tuple]) -> "DataTree":
        """
        Instantiate from a `ForestQuery` syntax directory name which provides
        process and data specifications. Since `subset`s and `params` are both
        at the root level in `ForestQuery`, any names not in `param_names` are
        assumed to be part of `subset`.
        Args:
            str_: directory name
            params_names: names of process params

        Returns:
            inst: `DataTree` created from `ForestQuery` syntax directory name
        """
        inst = cls(params_names=params_names)
        str_list = cls._list_str_components(str_, inst.OPERATORS)
        inst._build_from_str_list(str_list)
        return inst

    def new_subset(self, tree: Optional[dict] = None):
        """Adds a new `Tree` to `self.dict["subset"]`"""
        self._new_tree("subset", tree)

    def new_filter(self, tree: Optional[dict] = None):
        """Adds a new `Tree` to `self.dict["filter"]`"""
        self._new_tree("filter", tree)

    def new_partition(self, tree: Optional[Union[str, set]] = None):
        """Adds a new `Tree` to `self.dict["partition"]`"""
        if tree is None:
            tree = dict()
        elif isinstance(tree, str):
            tree = {
                tree,
            }
        elif isinstance(tree, set):
            pass
        else:
            raise TypeError(
                f"Tree must be `str`, `set`, or `NoneType`, not {type(tree)}"
            )
        tree = {key: None for key in tree}
        self._new_tree("partition", tree)

    def merge(self, other):
        raise NotImplementedError()

    def _new_tree(self, compartment: str, tree: Optional[dict]):
        """
        Creates a new `Tree` in the specified compartment . The new filesystem now becomes the `active_tree`.
        Args:
            compartment: "subset", "filter", or "partition"
            tree: `dict` which is to be used to create new `Tree`
        """
        if isinstance(self.active_tree, Tree):
            if self.active_tree.dict:
                self.active_tree = self.active_tree.copy_current_branch()
                if tree:
                    update_recursive(self.active_tree.dict, tree, inplace=True)
            else:
                self.active_tree = Tree(tree)
        else:
            self.active_tree = Tree(tree)
        getattr(self, compartment).append(self.active_tree)

    def _merge(self, input_dict: dict, param_names: Union[tuple, list]):
        """
        Merges values in `Spec` structured `input_dict` into `self`. Since
        `subset`s and `params` are both at root level, any keys not specified
        in `param_names` will be added to `subset` rather than `params`.
        Args:
            input_dict: dict to merge with `self`
            param_names: names of root level keys which are to be used as
                `params` rather than `subset`s
        """
        dict_ = deepcopy(input_dict)
        if "filter" in dict_:
            self.new_filter(dict_["filter"])
            del dict_["filter"]
        if "partition" in input_dict:
            self.new_partition(dict_["partition"])
            del dict_["partition"]
        self.params.dict.update({k: v for k, v in dict_.items() if k in param_names})
        if set(dict_.keys()).difference(param_names):
            self.subset.append(
                Tree({k: v for k, v in dict_.items() if k not in param_names})
            )

    def _build_from_str_list(self, str_list: List[str]):
        """
        After `ForestQuery` string has been parsed into component operators and
        words, use list of operators and words to build `Tree`
        Args:
            str_list: list of words and operators in `ForestQuery` operators
                and words
        """
        x_prev = str_list[0]
        if x_prev != self.FILTER and x_prev in self.OPERATORS:
            raise ValueError(f"Invalid start operator {x_prev}")
        for x in str_list:
            self._process_str(x, x_prev)
            x_prev = x

    def _process_str(self, x: str, x_prev: str):
        """
        Process a single operator or value as a string, setting up operation or
        adding value to self. Requires the context of the previous string,
        `x_prev` if it instructed downward traversal.
        Args:
            x: current string
            x_prev: previous string
        """
        if x == self.UP:
            if not self.active_tree.at_root:
                self.active_tree.up()
            else:
                self.active_tree = None
        elif x == self.DOWN:
            self.active_tree.down(x_prev)
        elif x == self.AND:
            pass
        elif x == self.FILTER:
            self.new_filter()
        elif x == self.PARTITION:
            self.new_partition()
        elif x == self.UNVERSIONED:
            self.unversioned = True
        elif x in self.param_names:
            self.active_tree = self.params
        else:
            if self.active_tree is None:
                self.new_subset()
            self.active_tree.add_child(x)

    @staticmethod
    def _list_str_components(
        dir_name_str: str, sep_chars: Tuple[str, ...] = OPERATORS
    ) -> List[str]:
        """
        Converts `ForestQuery` directory name string to a list of operators and
        values.
        Example:
            input: "eps:0.5-res:0.2-group:3"
            output: ["eps", ":", "0.5", "-", "res", ":", "0.2", "-",
                     "group", ":", 3]
        Args:
            dir_name_str: `ForestQuery` input string

        Returns:
            str_list: list of operators and values
        """
        str_list = []
        word = ""
        for char in dir_name_str:
            if char in sep_chars:
                if word != "":
                    str_list.append(word)
                str_list.append(char)
                word = ""
            else:
                word += char
        if word:
            str_list.append(word)
        return str_list

    @staticmethod
    def _tree_string(tree: Tree) -> str:
        """
        Converts a single `Tree` to a string in `ForestQuery` syntax. using
        `_helper` to recursively generate a string from `Tree.dict`.
        Args:
            tree: filesystem to be converted to string

        Returns:
            str_: `ForestQuery` string
        """
        str_ = ""

        def _helper(dict_: dict):
            nonlocal str_
            for key in sorted(dict_):
                val = dict_[key]
                str_ += str(key)
                if val is not None:
                    str_ += DataTree.DOWN
                if val is None:
                    pass
                elif isinstance(val, dict):
                    _helper(val)
                elif isinstance(val, (set, tuple)):
                    str_ += DataTree.AND.join(map(str, val))
                else:
                    str_ += str(val)
                str_ += DataTree.UP

        _helper(tree.dict)
        str_ = str_.strip("-")
        return str_

    def __str__(self):
        """
        Combines `ForestQuery` strings of individual `Tree`s to create one
        unified `ForestQuery` string. At the root level, `params` are listed
        first, followed by `subset`s, each in alphabetical order. Then
        `filter`s and `partition`s, each within their own keys, and organized
        alphabetically internally.
        """
        string = self._tree_string(self.params)
        subset_str = "--".join([self._tree_string(branch) for branch in self.subset])
        filter_str = "--!".join([self._tree_string(branch) for branch in self.filter])
        partition_str = "--@".join(
            [self._tree_string(branch) for branch in self.partition]
        )
        if subset_str:
            string += "-" + subset_str
        if filter_str:
            string += "--!" + filter_str
        if partition_str:
            string += "--@" + partition_str
        string = string.strip("-")
        return string

    def __repr__(self):
        return " ".join((self.__class__.__name__, str(self)))
