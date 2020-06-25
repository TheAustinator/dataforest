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


def tether(obj, tether_attr):
    """Tether a static method to a class instance"""
    tether_arg = getattr(obj, tether_attr)
    method_list = get_methods(obj)
    for method_name in method_list:
        method = getattr(obj, method_name)
        tethered_method = lambda: method(tether_arg)
        setattr(obj, method_name, tethered_method)
