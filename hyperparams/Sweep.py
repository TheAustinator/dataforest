import logging
from itertools import product
from typing import List, Callable, Dict, Any, TYPE_CHECKING, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np

if TYPE_CHECKING:
    from dataforest.ORM import DataForest


class Sweep:
    DEFAULT_SUBPLOT_SIZE = np.array((5, 5))

    def __init__(self, base_orm: "DataForest", sweep_dict: Dict[str, Dict[str, Any]], combinatorial: int = False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_orm = base_orm
        self.sweep_dict = sweep_dict
        self.combinatorial = combinatorial
        self._orm_matrix = None
        self._param_matrix = None

    @property
    def orm_matrix(self):
        if self._orm_matrix is None:
            if self.combinatorial:
                self._build_matrices_combinatorial()
            else:
                self._build_matrices()
        return self._orm_matrix

    @property
    def param_matrix(self):
        if self._param_matrix is None:
            if self.combinatorial:
                self._build_matrices_combinatorial()
            else:
                self._build_matrices()
        return self._param_matrix

    @property
    def indices(self):
        return list(product(*map(lambda x: list(range(x)), self.shape)))

    @property
    def shape(self):
        return self.orm_matrix.shape

    def run(
        self,
        methods: List[Callable],
        method_kwargs: Optional[List[dict]] = None,
        skip_if_done: Optional[str] = None,
        stop_on_error=False,
    ):
        if not method_kwargs:
            method_kwargs = len(methods) * [dict()]
        for (i, j) in self.indices:
            orm = self.orm_matrix[i, j]
            if orm is None:
                continue
            if skip_if_done:
                if orm[skip_if_done].done:
                    continue
            for (method, kwargs) in zip(methods, method_kwargs):
                try:
                    method(orm, **kwargs)
                except Exception as e:
                    if stop_on_error:
                        raise e
                    else:
                        msg = f"{e.__class__.__name__} raised on {self.param_matrix[i, j]}"
                        self.logger.warning(msg)

    def plot(
        self,
        plot_method: Callable,
        plot_method_kwargs: Optional[dict] = None,
        figsize: Optional[Tuple[int, int]] = None,
    ):
        if not plot_method_kwargs:
            plot_method_kwargs = dict()
        if figsize is None:
            figsize = tuple(self.DEFAULT_SUBPLOT_SIZE * np.array(self.shape))
        fig, ax = plt.subplots(*self.shape, sharex="col", sharey="row", figsize=figsize)
        for (i, j) in self.indices:
            orm = self.orm_matrix[i, j]
            if orm is None:
                continue
            params = self.param_matrix[i, j]
            plot = ax[i, j]
            plot.set_title(str(params))
            plot_method(orm, ax=plot, **plot_method_kwargs)

    def _build_matrices(self):
        orm_matrix = list()
        param_matrix = list()
        for process_name, param_dict in self.sweep_dict.items():
            for param_name, param_values in param_dict.items():
                orm_row = list()
                param_row = list()
                for value in param_values:
                    spec = self.base_orm.spec.copy()
                    spec[process_name][param_name] = value
                    orm = self.base_orm.copy(spec_dict=spec)
                    orm_row.append(orm)
                    param_row.append({param_name: value})
                orm_matrix.append(orm_row)
                param_matrix.append(param_row)
        self._fill_rows(orm_matrix)
        self._fill_rows(param_matrix)
        self._orm_matrix = np.array(orm_matrix)
        self._param_matrix = np.array(param_matrix)

    def _build_matrices_combinatorial(self):
        orm_matrix = list()
        param_matrix = list()
        assert len(self.sweep_dict) == 1
        for process_name, param_dict in self.sweep_dict.items():
            assert len(param_dict) == 2
            sweep_params = list(param_dict.items())
            (name_1, value_list_1) = sweep_params.pop()
            (name_2, value_list_2) = sweep_params.pop()
            for value_1 in value_list_1:
                orm_row = list()
                param_row = list()
                for value_2 in value_list_2:
                    spec = self.base_orm.spec.copy()
                    spec[process_name][name_1] = value_1
                    spec[process_name][name_2] = value_2
                    orm = self.base_orm.copy(spec_dict=spec)
                    orm_row.append(orm)
                    param_row.append({name_1: value_1, name_2: value_2})
                orm_matrix.append(orm_row)
                param_matrix.append(param_row)
        self._orm_matrix = np.array(orm_matrix)
        self._param_matrix = np.array(param_matrix)

    @staticmethod
    def _fill_rows(matrix):
        row_len = max(map(len, matrix))
        for row in matrix:
            row.extend((row_len - len(row)) * [None])
