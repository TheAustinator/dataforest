def get_methods(obj):
    """Get all user defined methods of an object"""
    all_methods = filter(lambda x: not x.startswith("__"), dir(obj))
    method_list = []
    for method_name in all_methods:
        try:
            if callable(getattr(obj, method_name)):
                method_list.append(str(method_name))
        except Exception:
            method_list.append(str(method_name))
    return method_list


def tether(obj, tether_attr, incl_methods=None, excl_methods=None):
    """Tether a static method to a class instance"""
    if incl_methods and excl_methods:
        ValueError("Cannot specify both `incl_methods` and `excl_methods`")
    tether_arg = getattr(obj, tether_attr)
    method_list = get_methods(obj)
    if excl_methods:
        method_list = [m for m in method_list if m not in excl_methods]
    elif incl_methods:
        method_list = incl_methods
    for method_name in method_list:
        method = getattr(obj, method_name)

        def tethered_method(*args, **kwargs):
            return method(tether_arg, *args, **kwargs)

        setattr(obj, method_name, tethered_method)
