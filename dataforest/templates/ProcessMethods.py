from dataforest.utils import tether


class ProcessMethods:
    """
    Container base class for `staticmethod`s which execute `processes` in a
    process system defined by a `ProcessSchema`. Methods should be decorated
    with the `dataprocess` hook to specify their upstream process dependencies
    so that the correct input data can be located and validated. Methods are
    also tethered to the class attached DataForest if the instantiated

    Method names must match `ProcessSchema`
    """

    def __init__(self, forest: "DataForest"):
        self.forest = forest
        tether(self, "forest")
