from copy import deepcopy
from typing import Any, Callable, Optional, Set, Tuple, Union

from dataforest.utils.exceptions import AscensionError, TraversalError


class Tree:
    """
    Tree data structure whose state can be described by a nested dictionary and
    a current node. The dictionary
    Attributes:
        dict: the nested `dict` which represents the filesystem structure. It
            also contains `set`s for a set of leaf nodes when the parents
            children are all leaf nodes. The non-container keys and values can
            be any datatype.
        stack: the series of keys used to access the current location in the
            nested `dict`s and `set`s. Can be thought of as the list of node
            names to get from the root to the current location in the filesystem
    """

    def __init__(self, tree: Optional[dict] = None, stack: Optional[list] = None):
        if tree is not None:
            if not isinstance(tree, dict):
                raise TypeError(
                    f"argument `filesystem` must be type `dict`, not {type(tree)}"
                )
        if stack is not None:
            if not isinstance(stack, list):
                raise TypeError(
                    f"argument `stack` must be type `list`, not {type(stack)}"
                )
        self.stack = []
        self.dict = dict()
        if tree is not None:
            self.dict = deepcopy(tree)
        if stack is not None:
            self.stack = stack
        # self.add_branch(stack)

    def up(self) -> list:
        """
        Move up to the parent of the current node, or outward in the dictionary
        Returns:
            self.stack: updated stack
        """
        if not self.stack:
            raise AscensionError(self.dict, self.stack)
        self.stack.pop()
        return self.stack

    def down(self, child: str = None) -> list:
        """
        Move down to a child of the current node, or inward in the dictionary.
        Child must be specified if there is more than one child node.
        Args:
            child: ???

        Returns:
            self.stack: updated stack
        """
        if child is None:
            child = self._get_only_child(self[self.stack])
        if self[self.stack] is not None:
            self.stack += [child]
        else:
            raise TraversalError(self.stack + [child], self.dict)
        return self.stack

    def add_sibling(self, val: Any, stack: list = None) -> list:
        """
        Add a new node at the same level as the specified node, which shares
        the same parent. Does not modify the current `self.stack`.
        Args:
            val: value for new node
            stack: stack for node to which to add sibling. If `None`, the
                current node at `self.stack` is used.

        Returns:
            sibling_stack: stack to newly added sibling node
        """
        if stack is None:
            stack = self.stack
        self[stack[:-1]] = val
        sibling_stack = stack[:-1] + [val]
        return sibling_stack

    def add_child(self, val: Any, stack: list = None) -> list:
        """
        Add a new child node to the specified node. Does not modify the current
        `self.stack`.
        Args:
            val: value for new node
            stack: stack for node to which to add child. If `None`, the current
                node at `self.stack` is used.

        Returns:
            child_stack: stack to newly added child node
        """
        if stack is None:
            stack = self.stack
        self[stack] = val
        child_stack = stack + [val]
        return child_stack

    def replace_node(self, stack: list, val: Any):
        """
        Replace the node at `stack` with `val`.
        Args:
            stack: path to node to be replaced
            val: value for new child node
        """
        container = self[stack[:-1]]
        child = stack[-1]
        if isinstance(container, dict):
            container.pop(child)
            container[val] = None
        else:
            parent = stack[-2]
            container = self[stack[:-2]]
            if isinstance(parent, set):
                parent.remove(child)
                parent.add(val)
            else:
                container[parent] = val

    def replace_parent(self, stack: list, val: Any):
        """
        Replace a node's parent node. Usually used to change container type
        Args:
            stack: Path to child node whose parent is to be replaced
            val: New parent node value
        """
        grand = self[stack[:-2]]
        grand_key = stack[-2]
        grand[grand_key] = val

    def pop(self, stack: list = None):
        """
        if stack is None:
            stack = self.stack
        parent = self[stack[:-2]]
        siblings = self[stack[:-1]]
        child =
        if len(parent) > 1:
        if isinstance(siblings, dict):
            val = siblings[child]
            if val is None:
                siblings.pop(child)
                return child
            else:
                siblings[child] = None
                return val
        elif isinstance(siblings, set):
            if child is not None:
                siblings.remove(child)
                return child
        else:
            self._replace_node(stack, None)
        """
        raise NotImplementedError()

    def apply_leaves(
        self, func: Callable, inplace: bool = False, **kwargs: Any
    ) -> "Tree":
        """
        Apply a specified function to the leaf nodes of the filesystem
        Args:
            func: specified function
            inplace: whether to apply to `self` or apply to a copy
            **kwargs: keyword arguments to pass to the specified function

        Returns:
            other: modified `Tree`
        """
        other = self if inplace else self.copy()
        for stack in self.depth_first_traverse():
            if self.is_leaf(stack):
                x = func(self[stack[:-1]], **kwargs)
                other.replace_node(stack, x)
        return other

    def list_leaves(self):
        """
        Lists values of leaves
        Returns:
            leaves: values of leaves
        """
        raise NotImplementedError()

    def add_branch(self, stack: list):
        """
        Recursively adds nodes in stack if they don't currently exist
        Args:
            stack: stack which includes all desired nodes to be added
        """
        if stack is not None:
            for key in stack:
                self.add_child(key)

    def copy_current_branch(self) -> "Tree":
        """
        Create a new filesystem with no nodes other than those in the current stack
        Returns:
            tree: new filesystem containing only current branch
        """
        tree = self.__class__()
        tree.add_branch(self.stack)
        return tree

    @property
    def paths(self) -> Set[tuple]:
        """
        Stacks to all nodes in `self`
        """
        return set(self.depth_first_traverse())

    @classmethod
    def from_paths(cls, paths):
        raise NotImplementedError()

    def union(self, *args: Union["Tree", dict]) -> Set[tuple]:
        """
        Union of `paths` between `self` and all others in `args`.
        """
        paths_list = self._paths_for_compare(self, *args)
        return set().union(*paths_list)

    def intersection(self, *args: Union["Tree", dict]) -> Set[tuple]:
        """
        Intersection of `paths` between self and all others in `args`.
        """
        paths_list = self._paths_for_compare(self, *args)
        return set.intersection(paths_list)

    def difference(self, *args: Union["Tree", dict]) -> Set[tuple]:
        """
        Paths in `self` which are not present in any others in `args`.
        """
        paths_list = self._paths_for_compare(self, args)
        return set.difference(paths_list)

    def difference_from_others_intersection(
        self, *args: Union["Tree", dict]
    ) -> Set[tuple]:
        """
        Paths in `self` which are not in the intersection of `args`.
        """
        self_paths = self._paths_for_compare(self)
        other_paths = self._paths_for_compare(*args)
        return self_paths.difference(set.intersection(other_paths))

    def issuperset(self, other: Union["Tree", dict]) -> bool:
        """
        Checks whether the paths in `self` are a superset of those in `other`.
        """
        self_paths, other_paths = self._paths_for_compare(self, other)
        return self_paths.issuperset(other_paths)

    def issubset(self, other: Union["Tree", dict]) -> bool:
        """
        Checks whether the paths in `self` are a subset of those in `other`.
        """
        self_paths, other_paths = self._paths_for_compare(self, other)
        return self_paths.issubset(other_paths)

    def variable_paths(self, *args: Union["Tree", dict]) -> Set[tuple]:
        """
        Paths which are not found in all `Trees` specified.
        """
        union = self.union(*args)
        paths_list = self._paths_for_compare(self, *args)
        variable_paths = set()
        [variable_paths.update(paths.difference(union)) for paths in paths_list]
        return variable_paths

    def variability_tree(self, *args: Union["Tree", dict]) -> "Tree":
        """
        A `Tree` built from `self.variable_paths`
        """
        return self.from_paths(self.variable_paths(*args))

    @property
    def at_root(self) -> bool:
        """
        Checks whether the current node is root
        """
        return len(self.stack) == 0

    @property
    def at_leaf(self) -> bool:
        """
        Checks whether the current node is a leaf node
        """
        return self.is_leaf(self.stack)

    def is_leaf(self, stack: list) -> bool:
        """
        Checks whether node at specified `stack` is a leaf node
        """
        if self[stack] is None:
            return True
        else:
            return False

    def copy(self) -> "Tree":
        """
        Deepcopy which maintains current node via `stack
        """
        tree = deepcopy(self.dict)
        stack = self.stack.copy()
        return self.__class__(tree=tree, stack=stack)

    def str_replace_leaves(self, old_str, new_str):
        def _str_replace(leaves):
            if isinstance(leaves, str):
                new_leaves = leaves.replace(old_str, new_str)
            elif isinstance(leaves, (list, tuple, set)):
                new_leaves = set()
                for leaf in leaves:
                    new_leaves.add(leaf.replace(old_str, new_str))
            else:
                new_leaves = leaves
            return new_leaves

        return self.apply_leaves(_str_replace)

    def depth_first_traverse(self) -> tuple:
        """
        Generator which runs entire traversal then yields values in depth first
        pre-order

        Yields:
            node_stack: stack to reach current node in traversal
        """
        # TODO: convert to JIT
        # TODO: alphabetical iteration?
        node_stacks = []

        def _recursor(stack):
            for key, val in self[stack].items():
                stack = self.down(key)
                node_stacks.append(tuple(stack))
                if val is None:
                    pass
                elif isinstance(val, dict):
                    _recursor(stack)
                elif isinstance(val, set):
                    for leaf in val:
                        node_stacks.append(tuple(stack + [leaf]))
                else:
                    node_stacks.append(tuple(stack + [val]))
                self.up()

        self.stack = []
        _recursor(self.stack)
        for node_stack in node_stacks:
            yield node_stack

    @staticmethod
    def _get_only_child(children: Any) -> Any:
        """
        A helper method to extract an only child. Raises an error if the parent
        has multiple children.
        Args:
            children: object or container to be checked for singularity. Unwrap
                if container

        Returns:
            children: unwrapped child node
        """
        if isinstance(children, (dict, set)):
            if len(children) > 1:
                raise ValueError(
                    f"Must specify `val` to pop when more than one child node"
                )
            return list(children)[0]
        else:
            return children

    def __str__(self) -> str:
        return str(self.dict)

    def __repr__(self) -> str:
        return str(self.__class__) + " " + str(self)

    def __getitem__(self, stack: list) -> Any:
        """
        Gets the children of the node specified by `stack`. Returns `self.dict`
        if `stack` is empty.
        Args:
            stack: path to parent node

        Returns:
            tree: children of parent
        """
        tree = self.dict
        if not stack:
            return tree
        for i, key in enumerate(stack):
            if not isinstance(tree, dict) and i == len(stack) - 1:
                return None
            if not isinstance(tree, dict):
                raise TraversalError(stack, tree)
            try:
                tree = tree[key]
            except TypeError:
                raise TraversalError(stack, tree)
        return tree

    def __setitem__(self, stack: list, val: Any):
        """
        Adds a child node with `val` to the node specified by `stack`. If
        `val` is a nested dict, all nodes will be added recursively.
        Args:
            stack: path to parent node
            val: value for new child node
        """
        if not stack:
            self.dict[val] = None
            return
        parent = self[stack[:-1]]
        key = stack[-1]
        if isinstance(parent, dict):
            if key not in parent:
                parent[key] = None
            node = parent[key]
            if node is None:
                parent[key] = val
            elif isinstance(node, set):
                node.add(val)
            elif isinstance(node, dict):
                node[val] = None
            else:
                parent[key] = {node, val}
        elif isinstance(parent, set):
            node = {k: None for k in parent}
            node[key] = val
            self.replace_parent(stack, node)
        else:
            node = {parent: val}
            self.replace_parent(stack, node)

    @staticmethod
    def _paths_for_compare(
        *args: Union[dict, "Tree"]
    ) -> Union[Tuple[Set[tuple]], Set[tuple]]:
        """
        Converts all `args` to `Tree`s and all node values to `str`, then
        extracts `Tree.paths` from each.
        Args:
            *args: trees to extract paths from

        Returns:
            tree_paths: sets in which each path is a tuple
        """
        tree_paths = list()
        for tree in args:
            if not isinstance(tree, Tree):
                tree = Tree(tree)
            paths = {tuple(map(str, path)) for path in tree.paths}
            tree_paths.append(paths)
        tree_paths = tuple(tree_paths) if len(tree_paths) > 1 else tree_paths[0]
        return tree_paths
