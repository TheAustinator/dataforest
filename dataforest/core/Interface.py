from pathlib import Path
from typing import Union, Optional, Iterable, List

import pandas as pd

from dataforest.core.DataBranch import DataBranch
from dataforest.core.DataTree import DataTree


class Interface:
    BRANCH_CLASS = DataBranch
    TREE_CLASS = DataTree

    @classmethod
    def load(
        cls,
        root: Union[str, Path],
        branch_spec: Optional[List[dict]] = None,
        tree_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        current_process: Optional[str] = None,
        remote_root: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> Union["DataBranch", "CellBranch", "DataTree", "CellTree"]:
        # TODO: replace kwargs with explicit to make it easier for users
        """
        Loads `cls.TREE_CLASS` if `tree_spec` passed, otherwise `cls.BRANCH_CLASS`
        Args:
            root:
            branch_spec:
            tree_spec:
            verbose:
            current_process:
            remote_root:

        Returns:

        """
        kwargs = {
            "branch_spec": branch_spec,
            "tree_spec": tree_spec,
            "verbose": verbose,
            "current_process": current_process,
            "remote_root": remote_root,
            **kwargs,
        }
        kwargs = cls._prune_kwargs(kwargs)
        interface_cls = cls._get_interface_class(kwargs)
        return interface_cls(root, **kwargs)

    @classmethod
    def from_input_dirs(
        cls,
        root: Union[str, Path],
        input_paths: Optional[Union[str, Path, Iterable[Union[str, Path]]]] = None,
        mode: Optional[str] = None,
        branch_spec: Optional[List[dict]] = None,
        tree_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        current_process: Optional[str] = None,
        remote_root: Optional[Union[str, Path]] = None,
        root_plots: bool = True,
        plot_kwargs: Optional[dict] = None,
        **kwargs,
    ) -> Union["DataBranch", "CellBranch", "DataTree", "CellTree"]:
        """
        # TODO: replace kwargs with explicit to make it easier for users
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataBranch is instantiated.
        The input directories are specified either via
        Args:
            root: root directory to deposit combined files
            input_paths: list of input data directories
            mode: ??? can be deleted?
            branch_spec:
            tree_spec:
            verbose:
            current_process:
            remote_root:
            root_plots:
            plot_kwargs:
        """
        if not isinstance(input_paths, (list, tuple)):
            input_paths = [input_paths]
        kwargs = {
            "branch_spec": branch_spec,
            "tree_spec": tree_spec,
            "verbose": verbose,
            "current_process": current_process,
            "remote_root": remote_root,
            **kwargs,
        }
        kwargs = cls._prune_kwargs(kwargs)
        interface_cls = cls._get_interface_class(kwargs)
        additional_kwargs = interface_cls._combine_datasets(root, input_paths=input_paths, mode=mode)
        kwargs = {**additional_kwargs, **kwargs}
        inst = interface_cls(root, **kwargs)
        if root_plots:
            inst.create_root_plots(plot_kwargs)
        return inst

    @classmethod
    def from_sample_metadata(
        cls,
        root: Union[str, Path],
        sample_metadata: Optional[pd.DataFrame] = None,
        branch_spec: Optional[List[dict]] = None,
        tree_spec: Optional[List[dict]] = None,
        verbose: bool = False,
        current_process: Optional[str] = None,
        remote_root: Optional[Union[str, Path]] = None,
        root_plots: bool = True,
        plot_kwargs: Optional[dict] = None,
        **kwargs,
    ) -> Union["DataBranch", "CellBranch", "DataTree", "CellTree"]:
        """
        # TODO: replace kwargs with explicit to make it easier for users
        Combines multiple datasets into a root directory, which will be the
        basis for downstream analysis. Then a DataBranch is instantiated.
        The input directories are specified either via
        Args:
            root: root directory to deposit combined files
            sample_metadata: path to metadata, where each row corresponds to a
                single dataset from `input_dirs`. The only column which must
                be present is `path`, which must be matched to the elements of
                `input_dirs`
            branch_spec:
            tree_spec:
            verbose:
            current_process:
            remote_root:
            root_plots:
            plot_kwargs: per-method plotting kwargs for methods in
                `PlotMethods` and subclasses.
                Ex: plot_kwargs = {
                        "plot_method_1": {
                            "y_lim": (0, 1),
                            "alpha": 0.5,
                        },
                        "plot_method_2": {
                            "y_var": "seq_depth",
                            "x_var": "donor_age",
                            "facet_by": "tissue_type",
                        },
                    }
        """
        kwargs = {
            "branch_spec": branch_spec,
            "tree_spec": tree_spec,
            "verbose": verbose,
            "current_process": current_process,
            "remote_root": remote_root,
            **kwargs,
        }
        kwargs = cls._prune_kwargs(kwargs)
        interface_cls = cls._get_interface_class(kwargs)
        additional_kwargs = interface_cls._combine_datasets(root, metadata=sample_metadata)
        kwargs = {**additional_kwargs, **kwargs}
        inst = interface_cls(root, **kwargs)
        if root_plots:
            inst.create_root_plots(plot_kwargs)
        return inst

    @staticmethod
    def _prune_kwargs(kwargs):
        return {k: v for k, v in kwargs.items() if v is not None}

    @classmethod
    def _get_interface_class(cls, kwargs):
        if "spec" in kwargs:
            raise ValueError(f"Keyword arg `spec` prohibited. Must provide either `branch_spec` or `tree_spec`")
        if "branch_spec" in kwargs and "tree_spec" in kwargs:
            raise ValueError(f"Cannot specify both `tree_spec` and `branch_spec`")
        return cls.TREE_CLASS if "tree_spec" in kwargs else cls.BRANCH_CLASS
