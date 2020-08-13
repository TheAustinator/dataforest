from copy import deepcopy

from dataforest.hooks.dataprocess import dataprocess


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
    if "plot_size" not in plot_kwargs:
        plot_kwargs["plot_size"] = "default"

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
    """Map kwargs options to values defined in kwargs defaults"""
    for key, values in plot_kwargs.items():
        if key in plot_kwargs_defaults:
            mapped_values = []
            for val in values:
                opts_mapping = plot_kwargs_defaults.get(key)
                if val in opts_mapping:
                    mapped_values.append(opts_mapping[val])
                else:
                    raise KeyError(f"Option '{val}' is not defined for '{key}', check `plot_kwargs_defaults`")

            plot_kwargs[key] = mapped_values

    return plot_kwargs


def _get_all_plot_kwargs(dp: dataprocess, plot_name):
    """Creates a list of dictionaries with singular value for each kwarg from kwarg value lists"""
    plot_kwargs_defaults = dp.branch.plot.plot_kwargs_defaults
    all_plot_kwargs = deepcopy(dp.branch.plot.plot_kwargs)  # make singular elements a list
    plot_kwargs = all_plot_kwargs.get(plot_name, {"plot_size": "default", "stratify": "none"})

    plot_kwargs = _unify_kwargs_opt_lens(plot_kwargs, plot_name)
    plot_kwargs = _map_kwargs_opts_to_values(plot_kwargs, plot_kwargs_defaults)

    return [
        dict(j) for j in zip(*[[(k, i) for i in v] for k, v in plot_kwargs.items()])
    ]  # 1-1 mapping of kwargs options
