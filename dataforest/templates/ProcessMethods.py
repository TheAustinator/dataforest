class ProcessMethods:
    def __init__(self, forest):
        self.forest = forest

    """
    Container base class for `staticmethod`s which execute `processes` in a
    process system defined by a `ProcessSchema`. Methods should be decorated
    with the `dataprocess` hook to specify their upstream process dependencies
    so that the correct input data can be located and validated.

    Method names must match `ProcessSchema`
    """

    pass
