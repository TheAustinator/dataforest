from typing import Union
from pathlib import Path

import numpy as np
import pandas as pd


class OntoPath(type(Path())):
    def is_parent(self, other: "OntoPath", fuzzy=False) -> bool:
        """Check if other is a parent of self"""
        if not fuzzy:
            return other in self.parents
        df = self._get_cmp_df(other)
        return self._is_fuzzy_match(df) and df.iloc[-1, 1] == "*"

    def is_child(self, other: "OntoPath", fuzzy=False) -> bool:
        """
        Check if other is a child of self
        Args:
            fuzzy: exact match not required if mismatch is with a `*`
        """
        if not fuzzy:
            return self in other.parents
        df = self._get_cmp_df(other)
        return self._is_fuzzy_match(df) and df.iloc[-1, 0] == "*"

    def is_related(self, other: "OntoPath", fuzzy=False) -> bool:
        return self == other or self.is_parent(other, fuzzy) or self.is_child(other, fuzzy)

    def is_related_fuzzy(self, other: "OntoPath"):
        return self.is_related(other, fuzzy=True)

    def tolist(self):
        return str(self).split("/")

    def to_series(self, drop_tail: bool = True) -> pd.Series:
        s = pd.Series(self.tolist())
        return self._drop_tail(s) if drop_tail else s

    def distance(self, other: "OntoPath"):
        """
        """
        n = len(self)
        df = self._get_cmp_df(other)
        matches = df.iloc[0, 0]._position_matches(df)
        return n - min(matches[~matches].index.min(), n) / n

    def _get_cmp_df(self, other: "OntoPath", drop_tail: bool = True) -> pd.DataFrame:
        """Get comparative dataframe for ancestry methods"""
        df = pd.DataFrame({"self": self, "other": other}, index=range(len(self)))
        return self._drop_tail(df) if drop_tail else df

    @staticmethod
    def _is_fuzzy_match(df: pd.DataFrame):
        return OntoPath._position_matches(df).all()

    @staticmethod
    def _position_matches(df):
        return (df.apply(lambda s: len(set(s)) == 1, axis=1)) | ((df == "*").any(axis=1))

    @staticmethod
    def _drop_tail(df: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
        # TODO: okay that this will non-tail stars?
        select = (df != "*").any(axis=1) if isinstance(df, pd.DataFrame) else df != "*"
        return df[select]

    def __sub__(self, other: "OntoPath") -> float:
        return self.distance(self, other)

    def __getitem__(self, k):
        return self.tolist()[k]

    def __len__(self):
        return len(self.tolist())

    def __invert__(self, other):
        return self.is_related(other, fuzzy=True)
