import logging
from copy import deepcopy
from typing import TYPE_CHECKING, Dict, Any, Optional

from IPython.display import Image, display
import ipywidgets as widgets
from matplotlib.figure import Figure

from dataforest.core.TreeSpec import TreeSpec

if TYPE_CHECKING:
    from dataforest.core.DataBranch import DataBranch
    from dataforest.core.DataTree import DataTree


class PlotWidget:
    def __init__(self, tree: "DataTree", plot_key: str, use_saved: bool = True, **plot_kwargs):
        """

        Args:
            tree:
            plot_key: key format of plot name (e.g. "_UMIS_VS_PERC_HSP_SCAT_")
            use_saved: use saved plots or regenerate
            **plot_kwargs:
        """
        self.branch = None
        self._tree = tree
        self._branch_spec_template = deepcopy(tree.tree_spec.branch_specs[0])
        self._branch_spec = deepcopy(self._branch_spec_template)
        self._plot_key = plot_key
        self._use_saved = use_saved
        self._bypass_kwargs = plot_kwargs.pop("bypass_kwargs", dict())
        self._plot_kwargs = plot_kwargs
        self._sweeps = self._tree.tree_spec.sweep_dict
        self._plot_cache = dict()

    def build_control(self, **plotter_kwargs):
        twig_lookup = self._tree.tree_spec.twig_lookup
        _kwargs = {}

        def _prep_sweeps_kwargs():
            process = self._tree.current_process
            precursors = self._tree.tree_spec.get_precursors_lookup(incl_current=True)[process]
            sweeps = set().union(*[self._sweeps[precursor] for precursor in precursors])
            # {"key:operation:process": value, ...} (e.g. {"num_pcs:_PARAMS_:normalize": 30, ...})
            param_sweeps = {":".join(swp[:3][::-1]): list(swp[3]) for swp in sweeps}
            _kwargs.update({**param_sweeps, **self._plot_kwargs})

        if self._tree.has_sweeps:
            _prep_sweeps_kwargs()
        if self._tree.has_twigs:
            _kwargs["twig_str"] = list(twig_lookup.keys())

        @widgets.interact(**_kwargs)
        # TODO: this seems to be slower now that the spec is recalculated every time -- is it replotted?
        def _control(**kwargs: Dict[str, Any]):
            spec = deepcopy(self._branch_spec_template)
            twig_str = kwargs.pop("twig_str", None)
            # add sweeps
            for param_keys_str, value in kwargs.items():
                if param_keys_str in self._plot_kwargs:
                    self._plot_kwargs[param_keys_str] = value
                    continue
                elif isinstance(value, float):
                    value = int(value) if int(value) == value else value
                (name, operation, process) = param_keys_str.split(":")
                spec[process][operation][name] = value
            # TODO: do we need to do something with kwargs? Or taken care of?
            [print(k, kwargs.pop(k, None)) for k in self._plot_kwargs]
            # add twigs
            if twig_str:
                twig = twig_lookup[twig_str]
                spec = TreeSpec.add_twig(spec, twig)
            # get branch
            self.branch = self._tree._branch_cache[str(spec)]
            return self._get_plot(self.branch, **plotter_kwargs)

        return _control

    def _get_plot(self, branch, **kwargs):
        kwargs = {**self._bypass_kwargs, **kwargs}
        plot_map = branch[self._tree.current_process]._plot_map
        plot_path_lookup = {plot_key: next(iter(path_dict.values())) for plot_key, path_dict in plot_map.items()}
        spec = branch.spec[: branch.current_process]
        cache_key = str({"spec": spec, "plot_kwargs": self._plot_kwargs})
        if self._plot_key in plot_path_lookup and self._use_saved:
            plot_path = plot_path_lookup.get(self._plot_key)
            if plot_path.exists():
                return Image(plot_path)
        generated = False
        plot_obj = self._plot_cache.get(cache_key, None)
        if not plot_obj:
            generated = True
            plot_obj = self._generate_plot(branch, **kwargs)
            self._plot_cache[cache_key] = plot_obj
        if isinstance(plot_obj, tuple) and isinstance(plot_obj[0], Figure):
            if not generated:
                return plot_obj[0]
        elif isinstance(plot_obj, Image):
            return plot_obj
        elif isinstance(plot_obj, list) and all(isinstance(x, Image) for x in plot_obj):
            for img in plot_obj:
                display(img)
            return plot_obj
        else:
            raise TypeError(
                f"Expected types (matplotlib.axes.Axes, IPython.display.Image, List[Ipython.display.Image]). Got {type(plot_obj)}"
            )

    def _generate_plot(self, branch: "DataBranch", **kwargs):
        method = branch.plot.method_lookup[self._plot_key]
        kwargs = {**self._plot_kwargs, **kwargs, "show": False}
        return method(**kwargs)
