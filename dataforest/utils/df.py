import numpy as np
import pandas as pd


def bin_col(df, col, bins):
    """Bins values in column and uses the bin center as a label"""
    bins = np.array(bins)
    labels = (bins[1:] + bins[:-1]) / 2
    return pd.cut(df[col], bins=bins, labels=labels)
