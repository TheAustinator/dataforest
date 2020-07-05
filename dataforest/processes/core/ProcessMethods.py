from dataforest.processes.core.MetaProcessMethods import MetaDataProcess
from dataforest.utils import tether


class ProcessMethods(metaclass=MetaDataProcess):
    """
    Container base class for `staticmethod`s which execute `processes` in a
    processes system defined by a `ProcessSchema`. Methods should be decorated
    with the `dataprocess` hook to specify their upstream processes dependencies
    so that the correct input data can be located and validated. Methods are
    also tethered to the class attached DataForest if the instantiated

    Method names must match `ProcessSchema`
    """

    def __init__(self, forest: "DataForest"):
        self.forest = forest
        for process_name, process in self.__class__.PROCESSES.items():
            setattr(self, process_name, process)
        tether(self, "forest")
