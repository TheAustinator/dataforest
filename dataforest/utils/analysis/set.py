def iou(*args):
    args = list(map(set, args))
    return len(set.intersection(*args)) / len(set.union(*args))


def iom(*args):
    """intersection over min size collection"""
    args = list(map(set, args))
    min_size = min(list(map(len, args)))
    return len(set.intersection(*args)) / min_size


def iof(*args):
    """intersection over size of first arg"""
    return len(set.intersection(*args)) / len(args[0])
