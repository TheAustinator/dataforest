from functools import wraps
from typing import Callable

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity as _cosine_similarity, cosine_distances as _cosine_distances, euclidean_distances as _euclidean_distances, manhattan_distances as _manhattan_distances


def _wrap_sk_metric(f: Callable) -> Callable:
    @wraps(f)
    def wrapper(*args, **kwargs):
        args = list(args)
        for i, arg in enumerate(args):
            if isinstance(arg, np.ndarray) and arg.ndim == 1:
                args[i] = arg.reshape(1, -1)
        res = f(*args, **kwargs)
        return res[0, 0]
    return wrapper


cosine_sim = _wrap_sk_metric(_cosine_similarity)
cosine_dist = _wrap_sk_metric(_cosine_distances)
euclidean_dist = _wrap_sk_metric(_euclidean_distances)
manhattan_dist = _wrap_sk_metric(_manhattan_distances)


def intersection(s1, s2): return len(s1.intersection(s2))
