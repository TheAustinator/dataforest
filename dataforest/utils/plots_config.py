from copy import deepcopy
import json
from collections import OrderedDict
from typing import Dict


def build_process_plot_method_lookup(plot_map: dict) -> Dict[str, Dict[str, str]]:
    """
    Get a lookup of processes, each containing a mapping between plot method
    names in the config and the actual callable names.
    Format:
        process_name[config_plot_name][plot_callable_name]
    Ex:
        {"normalize": {"_UMIS_PER_CELL_HIST_": "plot_umis_per_cell_hist", ...}, ...}
    """
    process_plot_methods = {}
    for process, plots in plot_map.items():
        process_plot_methods[process] = {}
        for plot_name in plots.keys():
            try:
                plot_method = plots[plot_name]["plot_method"]
            except (TypeError, KeyError):
                plot_method = _get_plot_method_from_plot_name(plot_name)

            process_plot_methods[process][plot_name] = plot_method
    return process_plot_methods


def parse_plot_kwargs(plot_map: dict, plot_kwargs_defaults: dict):
    """Parse plot methods plot_kwargs per process from plot_map"""
    all_plot_kwargs = {}
    for process, plots in plot_map.items():
        all_plot_kwargs[process] = {}

        for plot_name in plots.keys():
            all_plot_kwargs[process][plot_name] = {}

            try:
                plot_kwargs = plots[plot_name]["plot_kwargs"]
            except (KeyError, TypeError):
                plot_kwargs = _get_default_plot_kwargs(plot_kwargs_defaults)

            kwargs_feed = _get_plot_kwargs_feed(plot_kwargs, plot_kwargs_defaults, plot_name)
            kwargs_feed_mapped = _get_plot_kwargs_feed(
                plot_kwargs, plot_kwargs_defaults, plot_name, map_to_default_kwargs=True,
            )

            for plot_kwargs_set, plot_kwargs_set_mapped in zip(kwargs_feed, kwargs_feed_mapped):
                all_plot_kwargs[process][plot_name][_get_plot_kwargs_string(plot_kwargs_set)] = plot_kwargs_set_mapped

    return all_plot_kwargs


def parse_plot_map(plot_map: dict, plot_kwargs_defaults: dict):
    """
    Parse plot file map per process from plot_map and ensures that
    implicit definition returns a dictionary of default values for all plot_kwargs
    """
    all_plot_maps = {}
    for process, plots in plot_map.items():
        all_plot_maps[process] = {}

        for plot_name in plots.keys():
            all_plot_maps[process][plot_name] = {}

            try:
                all_plot_kwargs = plots[plot_name]["plot_kwargs"]
            except (KeyError, TypeError):
                all_plot_kwargs = _get_default_plot_kwargs(plot_kwargs_defaults)

            kwargs_feed = _get_plot_kwargs_feed(all_plot_kwargs, plot_kwargs_defaults, plot_name)
            kwargs_feed_mapped = _get_plot_kwargs_feed(
                plot_kwargs=all_plot_kwargs,
                plot_kwargs_defaults=plot_kwargs_defaults,
                plot_name=plot_name,
                map_to_default_kwargs=True,
            )

            for i, (plot_kwargs_set, plot_kwargs_set_mapped) in enumerate(zip(kwargs_feed, kwargs_feed_mapped)):
                try:
                    plot_filename = plots[plot_name]["filename"]
                    if type(plot_filename) == list:
                        plot_filename = _get_formatted_plot_filename(plot_filename[i], plot_kwargs_defaults)
                except (KeyError, TypeError):
                    plot_filename = _get_default_plot_filename(plot_name, plot_kwargs_set_mapped, plot_kwargs_defaults)

                all_plot_maps[process][plot_name][_get_plot_kwargs_string(plot_kwargs_set)] = plot_filename
    return all_plot_maps


def _get_plot_method_from_plot_name(plot_name):
    """Infer plot method name from plot name, e.g. _UMIS_PER_CELL_HIST_ -> plot_umis_per_cell_hist"""
    if plot_name[0] == "_":
        plot_name = plot_name[1:]
    if plot_name[-1] == "_":
        plot_name = plot_name[:-1]
    formatted_plot_name = plot_name.lower()
    plot_method = "plot_" + formatted_plot_name

    return plot_method


def _unify_kwargs_opt_lens(plot_kwargs: dict, plot_kwargs_defaults: dict, plot_name: str):
    """
    Make all kwarg option counts equal so that we can get aligned in order options

    Examples:
        >>> _unify_kwargs_opt_lens(
        >>>     {
        >>>         "stratify": ["sample_id", "none"],
        >>>         "plot_size": "default"
        >>>     },
        >>>     plot_kwargs_defaults, plot_name
        >>> )
        # output
        {
            "stratify": ["sample_id", "none"],
            "plot_size": ["default", "default"]
        }
    """
    kwargs_num_options = set()  # number of options for each kwarg
    for key, values in plot_kwargs.items():
        if type(values) != list:
            plot_kwargs[key] = [values]
        else:
            kwargs_num_options.add(len(values))
    try:
        max_num_opts = max(kwargs_num_options)
        kwargs_num_options.remove(max_num_opts)
    except ValueError:
        max_num_opts = 1  # means that there are no lists and just singular arguments
    if len(kwargs_num_options) > 0:  # check if the lists of options are equal to each other
        raise ValueError(
            f"'{plot_name}' contains arguments with unequal number of options, should include the same number of options where there are multiple options or a single option."
        )

    # fill in plot_kwargs that are not defined
    template_plot_kwargs = _get_default_plot_kwargs(plot_kwargs_defaults)
    for key, value in template_plot_kwargs.items():
        if key not in plot_kwargs:
            plot_kwargs[key] = value

    for key, values in plot_kwargs.items():
        if type(values) != list:
            plot_kwargs[key] = [values] * max_num_opts  # stretch to the same length
        elif len(values) == 1:
            plot_kwargs[key] = values * max_num_opts

    return plot_kwargs


def _map_kwargs_opts_to_values(plot_kwargs, plot_kwargs_defaults):
    """Map plot_kwargs to values defined in plot_kwargs defaults if available"""
    mapped_plot_kwargs = deepcopy(plot_kwargs)

    for kwarg_name, kwarg_values in plot_kwargs.items():
        if kwarg_name in plot_kwargs_defaults:
            mapping = []
            for val in kwarg_values:
                try:
                    mapping.append(plot_kwargs_defaults[kwarg_name].get(val, val))
                except TypeError:
                    mapping.append(val)

            mapped_plot_kwargs[kwarg_name] = mapping

    return mapped_plot_kwargs


def _get_plot_kwargs_feed(plot_kwargs: dict, plot_kwargs_defaults: dict, plot_name: str, map_to_default_kwargs=False):
    plot_kwargs = _unify_kwargs_opt_lens(plot_kwargs, plot_kwargs_defaults, plot_name)
    if map_to_default_kwargs:
        plot_kwargs = _map_kwargs_opts_to_values(plot_kwargs, plot_kwargs_defaults)
    plot_kwargs_feed = [
        dict(j) for j in zip(*[[(k, i) for i in v] for k, v in plot_kwargs.items()])
    ]  # 1-1 mapping of plot_kwargs options

    return plot_kwargs_feed


def _get_default_plot_kwargs(plot_kwargs_defaults: dict):
    kwargs_keys = list(plot_kwargs_defaults.keys())
    for kwargs_key in reversed(kwargs_keys):
        if "filename" in kwargs_key:  # ignore filename-related args (e.g., plot filename extension)
            kwargs_keys.remove(kwargs_key)

    default_plot_kwargs = dict(zip(kwargs_keys, ["default"] * len(kwargs_keys)))

    return default_plot_kwargs


def plot_kwargs_to_str(plot_kwargs):
    """
    Converts plot_kwargs dictionary into a deterministic string, sorted by keys
    """
    UP = "-"
    DOWN = ":"
    AND = "+"
    str_chain = ""

    def _helper(dict_: dict):
        nonlocal str_chain
        for key in sorted(dict_):
            val = dict_[key]
            str_chain += str(key)
            if val is not None:
                str_chain += DOWN
            if val is None:
                pass
            elif isinstance(val, dict):
                _helper(val)
            elif isinstance(val, (set, list, tuple)):
                str_chain += AND.join(map(str, val))
            else:
                str_chain += str(val)
            str_chain += UP

    _helper(plot_kwargs)
    str_chain = str_chain.strip("-").lower()

    return str_chain


def _get_plot_kwargs_string(plot_kwargs: dict):  # TODO-QC: proper type checking
    ord_plot_kwargs = OrderedDict(sorted(plot_kwargs.items()))

    return json.dumps(ord_plot_kwargs)


def _get_formatted_plot_filename(plot_name: str, plot_kwargs_defaults: dict):
    filename_ext = "." + plot_kwargs_defaults.get("filename_ext", "png").lower().replace(".", "")

    plot_filename = plot_name.lower()
    if "." not in plot_name:  # if plot map doesn't have extension yet
        plot_filename += filename_ext

    return plot_filename


def _get_default_plot_filename(plot_name: str, plot_kwargs: dict, plot_kwargs_defaults: dict):
    """Infer plot filename from plot name, e.g. _UMIS_PER_CELL_HIST_ -> umis_per_cell_hist.png"""
    filename_ext = "." + plot_kwargs_defaults.get("filename_ext", "png").lower().replace(".", "")

    if plot_name[0] == "_":
        plot_name = plot_name[1:]
    if plot_name[-1] == "_":
        plot_name = plot_name[:-1]
    plot_filename = plot_name.lower() + "-" + plot_kwargs_to_str(plot_kwargs) + filename_ext

    return plot_filename
