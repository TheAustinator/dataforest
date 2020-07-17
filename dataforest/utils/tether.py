from functools import wraps


def tether(obj, tether_attr, incl_methods=None, excl_methods=None):
    """
    Tether a static method to a class instance, supplying a specified
    `tether_arg` as the first argument, implicitly.
    """
    if incl_methods and excl_methods:
        ValueError("Cannot specify both `incl_methods` and `excl_methods`")
    tether_arg = getattr(obj, tether_attr)
    method_list = get_methods(obj, incl_methods, excl_methods)
    for method_name in method_list:
        method = getattr(obj, method_name)
        tethered_method = make_tethered_method(method, tether_arg)
        setattr(obj, method_name, tethered_method)


def get_methods(obj, excl_methods=None, incl_methods=None):
    """Get all user defined methods of an object"""
    all_methods = filter(lambda x: not x.startswith("__"), dir(obj))
    method_list = []
    for method_name in all_methods:
        if hasattr(obj, method_name):
            if callable(getattr(obj, method_name)):
                method_list.append(method_name)
    if excl_methods:
        method_list = [m for m in method_list if m not in excl_methods]
    elif incl_methods:
        method_list = incl_methods
    return method_list


def make_tethered_method(method, tether_arg):
    @wraps(method)
    def tethered_method(*args, **kwargs):
        return method(tether_arg, *args, **kwargs)

    return tethered_method
