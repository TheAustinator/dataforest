from dataforest.utils import tether


class PlotMethods:
    def __init__(self, branch: "DataBranch"):
        self.branch = branch
        tether(self, "branch")
