from typing import Optional

from dataforest.utils.utils import order_dict


class RunSpec(dict):
    """
    Specification to run a single process.
    Keys:
        process (str): name of the `dataprocess` decorated function to execute
        alias (Optional[str]): given name, which is required when multiple of
            the same `process` are to exist in the same `BranchSpec`
        params: parameters for `process`
        subset, filter, partition: see dataforest.core.DataBranch docs
    """

    @property
    def name(self) -> str:
        return self.get("_ALIAS_", self["_PROCESS_"])

    @property
    def process(self) -> str:
        return self["_PROCESS_"]

    @property
    def alias(self) -> Optional[str]:
        return self.get("_ALIAS_", None)

    @property
    def params(self) -> dict:
        return self.get("_PARAMS_", {})

    @property
    def subset(self) -> dict:
        return self.get("_SUBSET_", {})

    @property
    def filter(self) -> dict:
        return self.get("_FILTER_", {})

    @property
    def partition(self) -> dict:
        return self.get("_PARTITION_", {})

    def ordered(self) -> dict:
        """Order dict alphabetically for deterministic string representation"""
        return order_dict(self)

    def __str__(self):
        return str(self.ordered())
