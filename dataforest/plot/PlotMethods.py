from dataforest.utils import tether


class PlotMethods:
    def __init__(self, forest: "DataForest"):
        self.forest = forest
        tether(self, "forest")
