from dataforest.core.DataBranch import DataBranch


class DataTree:
    BRANCH_CLASS = DataBranch

    def __init__(
        self, root_dir,
    ):
        raise NotImplementedError()
