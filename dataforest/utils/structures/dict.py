def map_dict_vals(dict_, func, *args, pass_keys=False, print_keys=False, **kwargs):
    d = dict()
    for k, v in dict_.items():
        if print_keys:
            print(k)
        _args = (k, v, *args) if pass_keys else (v, *args)
        d[k] = func(*_args, **kwargs)
    return d
