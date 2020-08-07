import numpy as np


class Sweep(list):
    def __init__(self, operation, key, sweep_obj):
        self.operation = operation
        self.key = key
        super().__init__(self._get_values(sweep_obj))

    @staticmethod
    def _get_values(sweep_obj):
        if isinstance(sweep_obj, dict):
            min_ = sweep_obj["min"]
            max_ = sweep_obj["max"]
            if "base" in sweep_obj:
                base = sweep_obj["base"]
                num = sweep_obj.get("num", max_ - min_ + 1)
                values = np.logspace(min_, max_, num, base=base)
            elif "step" in sweep_obj:
                step = sweep_obj["step"]
                values = np.arange(min_, max_ + step, step)
            else:
                raise ValueError(f'If _SWEEP_ is a dict, it must contain either "base" for log or "step" for linear')
            all_ints = all(map(lambda x: float(x).is_integer(), values))
            dtype = int if all_ints else float
            values = list(map(dtype, values))
        elif isinstance(sweep_obj, (list, set, tuple)):
            values = list(sweep_obj)
        else:
            raise TypeError(f"`sweep_obj` must be of types: [dict, list, set, tuple]")
        return values

    def __str__(self):
        return f"Sweep<{super().__repr__()[1:-1]}>"

    def __repr__(self):
        return str(self)
