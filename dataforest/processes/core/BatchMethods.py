import logging
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt

if TYPE_CHECKING:
    from dataforest.core.DataForest import DataForest


class BatchMethods:
    logger = logging.getLogger()

    def __init__(self, forest: "DataForest"):
        self.forest = forest

    @staticmethod
    def plot_sweep(self, sweep_dict, process_name, plot_method):
        grid_dims = (max(map(len, sweep_dict.values())), len(sweep_dict))
        fig, ax = plt.subplots(*grid_dims, sharex="col", sharey="row", figsize=(25, 40))
        raise NotImplementedError()
