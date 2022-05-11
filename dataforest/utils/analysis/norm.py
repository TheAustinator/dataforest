from typing import Union

import numpy as np
import pandas as pd


def log1p_sign(arr: Union[pd.Series, np.ndarray]):
    arr = pd.Series(arr.copy())
    arr[arr > 0] = np.log(1 + arr[arr > 0])
    arr[arr < 0] = -np.log(1 + arr[arr < 0].abs())
    return arr
