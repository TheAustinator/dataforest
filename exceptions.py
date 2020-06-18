class TraversalError(ValueError):
    def __init__(self, tree, stack):
        self._tree = tree
        self._stack = stack

    def __str__(self):
        return f"Cannot access stack: {self._stack} in tree: {self._tree}"


class AscensionError(TraversalError):
    def __str__(self):
        return f"attempted to ascend beyond root of tree: {self._tree}"


class InputDataNotFound(FileNotFoundError):
    def __init__(self, orm, requires, process_name):
        self._orm = orm
        self._requires = requires
        self._process_name = process_name

    def __str__(self):
        return (
            f"Process `{self._process_name}` requires input data from `{self._requires}`. Not found at expected"
            f"location: {self._orm.paths[self._requires]} based on `orm.spec`: {self._orm.spec}"
        )


class UnnecessaryKeysError(KeyError):
    def __init__(self, dict_, process_name: str = None):
        self.dict = dict_
        self.process_name = process_name

    def __str__(self):
        end_str = f" - {self.dict}. Please remove unnecessary keys from `ORM`."
        if self.process_name:
            str_ = f"Unnecessary key(s) for `process_name`: {self.process_name} "
        else:
            str_ = f"Unnecessary key(s) "
        return str_ + end_str
