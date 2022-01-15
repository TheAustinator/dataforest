def map_dict_vals(dict_, func, *args, pass_keys=False, pass_enum=False, print_keys=False, **kwargs):
    d = dict()
    for i, (k, v) in enumerate(dict_.items()):
        if print_keys:
            print(k)
        _args = [v, *args]
        if pass_keys:
            _args.insert(0, k)
        if pass_enum:
            _args.insert(0, i)
        d[k] = func(*_args, **kwargs)
    return d
