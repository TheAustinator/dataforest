def map_dict_vals(dict_, func, print_keys=False):
    d = dict()
    for k, v in dict_.items():
        if print_keys:
            print(k)
        d[k] = func(v)
    return d
