import pandas as pd


class DataFrameModel:
    DIMENSIONS = ("x", "y", "color", "facet_col", "facet_row")
    MARKER_ARGS = ["opacity", "size"]
    MARKER_ARGS_PARAMS = {
        "opacity": {"min": 0, "max": 1, "step": 0.05, "value": 0.25},
        "size": {"min": 1, "max": 25, "step": 1, "value": 5},
    }

    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def df(self):
        return self._df

    @property
    def col_options(self):
        return [dict(label=x, value=x) for x in self.df.columns]
