from copy import deepcopy
import json
from collections import OrderedDict
from pathlib import Path


def parse_plot_methods(config: dict):
    """Parse plot methods per process from plot_map"""
    plot_map = config["plot_map"]
    plot_methods = {}
    for process, plots in plot_map.items():
        plot_methods[process] = {}
        for plot_name in plots.keys():
            try:
                plot_method = plots[plot_name]["plot_method"]
            except (TypeError, KeyError):
                plot_method = _get_plot_method_from_plot_name(plot_name)

            plot_methods[process][plot_name] = plot_method

    return plot_methods


def parse_plot_kwargs(config: dict):
    """Parse plot methods kwargs per process from plot_map"""
    plot_map = config["plot_map"]
    all_plot_kwargs = {}
    for process, plots in plot_map.items():
        all_plot_kwargs[process] = {}

        for plot_name in plots.keys():
            all_plot_kwargs[process][plot_name] = {}

            try:
                plot_kwargs = plots[plot_name]["plot_kwargs"]
            except (KeyError, TypeError):
                plot_kwargs = _get_default_plot_kwargs(config)

            kwargs_feed = _get_plot_kwargs_feed(plot_kwargs, plot_name)
            kwargs_feed_mapped = _get_plot_kwargs_feed(
                plot_kwargs, plot_name, map_to_default_kwargs=True, plot_kwargs_defaults=config["plot_kwargs_defaults"]
            )

            for plot_kwargs_set, plot_kwargs_set_mapped in zip(kwargs_feed, kwargs_feed_mapped):
                all_plot_kwargs[process][plot_name][_get_plot_kwargs_string(plot_kwargs_set)] = plot_kwargs_set_mapped

    return all_plot_kwargs


def parse_plot_map(config: dict):
    """
    Parse plot file map per process from plot_map and ensures that
    implicit definition returns a dictionary of default values for all kwargs
    """
    plot_map = config["plot_map"]
    all_plot_maps = {}
    for process, plots in plot_map.items():
        all_plot_maps[process] = {}

        for plot_name in plots.keys():
            all_plot_maps[process][plot_name] = {}

            try:
                all_plot_kwargs = plots[plot_name]["plot_kwargs"]
            except (KeyError, TypeError):
                all_plot_kwargs = _get_default_plot_kwargs(config)

            kwargs_feed = _get_plot_kwargs_feed(all_plot_kwargs, plot_name)
            kwargs_feed_mapped = _get_plot_kwargs_feed(
                all_plot_kwargs,
                plot_name,
                map_to_default_kwargs=True,
                plot_kwargs_defaults=config["plot_kwargs_defaults"],
            )

            for i, (plot_kwargs_set, plot_kwargs_set_mapped) in enumerate(zip(kwargs_feed, kwargs_feed_mapped)):
                try:
                    plot_filename = plots[plot_name]["filename"]
                    if type(plot_filename) == list:
                        plot_filename = Path(plot_filename[i])
                except (KeyError, TypeError):
                    plot_filename = _get_default_plot_filename(
                        plot_name, plot_kwargs_set_mapped, config["plot_kwargs_defaults"]
                    )

                all_plot_maps[process][plot_name][_get_plot_kwargs_string(plot_kwargs_set)] = plot_filename

    return all_plot_maps


def get_plot_name_from_plot_method(process_plot_methods, plot_method_name):
    """Reverse search for plot name in the config from plot method used"""
    for key, value in process_plot_methods.items():
        if value == plot_method_name:
            plot_name = key  # look up plot name from plot_method name

    return plot_name


def _get_plot_method_from_plot_name(plot_name):
    """Infer plot method name from plot name, e.g. _UMIS_PER_CELL_HIST_ -> plot_umis_per_cell_hist"""
    if plot_name[0] == "_":
        plot_name = plot_name[1:]
    if plot_name[-1] == "_":
        plot_name = plot_name[:-1]
    formatted_plot_name = plot_name.lower()
    plot_method = "plot_" + formatted_plot_name

    return plot_method


def _unify_kwargs_opt_lens(plot_kwargs, plot_name):
    """
    Make all kwarg option counts equal so that we can get aligned in order options

    Examples:
        >>> _unify_kwargs_opt_lens(
        >>>     {
        >>>         "stratify": ["sample", "none"],
        >>>         "plot_size": "default"
        >>>     }
        >>> )
        # output
        {
            "stratify": ["sample", "none"],
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
            f"plot_kwargs['{plot_name}'] contains arguments with unequal number of options, should include the same number of options where there are multiple options or a single option."
        )

    for key, values in plot_kwargs.items():
        if type(values) != list:
            plot_kwargs[key] = [values] * max_num_opts  # stretch to the same length
        elif len(values) == 1:
            plot_kwargs[key] = values * max_num_opts

    return plot_kwargs


def _map_kwargs_opts_to_values(plot_kwargs, plot_kwargs_defaults):
    """Map plot_kwargs to values defined in kwargs defaults if available"""
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


def _get_plot_kwargs_feed(plot_kwargs: dict, plot_name: str, map_to_default_kwargs=False, plot_kwargs_defaults=None):
    plot_kwargs = _unify_kwargs_opt_lens(plot_kwargs, plot_name)
    if map_to_default_kwargs:
        plot_kwargs = _map_kwargs_opts_to_values(plot_kwargs, plot_kwargs_defaults)
    plot_kwargs_feed = [
        dict(j) for j in zip(*[[(k, i) for i in v] for k, v in plot_kwargs.items()])
    ]  # 1-1 mapping of kwargs options

    return plot_kwargs_feed


def _get_default_plot_kwargs(config: dict):
    kwargs_keys = config["plot_kwargs_defaults"].keys()
    default_plot_kwargs = dict(zip(kwargs_keys, ["default"] * len(kwargs_keys)))

    return default_plot_kwargs


def _get_filename_from_plot_kwargs(plot_filename, plot_kwargs):
    suffix_chain = ""
    for key, value in sorted(list(plot_kwargs.items())):
        suffix_chain += f"-{key}:{value}".replace(" ", "").lower()  # remove spaces

    return plot_filename + suffix_chain


def _get_plot_kwargs_string(plot_kwargs: dict):  # TODO-QC: proper type checking
    ord_plot_kwargs = OrderedDict(sorted(plot_kwargs.items()))

    return json.dumps(ord_plot_kwargs)


def _get_default_plot_filename(plot_name: str, plot_kwargs: dict, plot_kwargs_defaults: dict):
    """Infer plot filename from plot name, e.g. _UMIS_PER_CELL_HIST_ -> umis_per_cell_hist.png"""
    filename_ext = "." + plot_kwargs_defaults.get("filename_ext", "png").lower().replace(".", "")

    if plot_name[0] == "_":
        plot_name = plot_name[1:]
    if plot_name[-1] == "_":
        plot_name = plot_name[:-1]
    plot_filename = _get_filename_from_plot_kwargs(plot_name.lower(), plot_kwargs) + filename_ext

    return Path(plot_filename)
