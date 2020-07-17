class HashCash(dict):
    def __init__(self):
        # TODO: allow get function dynamically passed here?
        super().__init__()
        self._cache = dict()

    def keys(self):
        return self._cache.keys()

    def __getitem__(self, k):
        if k not in self._cache:
            self._cache[k] = self._get(k)
        elif self._cache[k] is None:
            self._cache[k] = self._get(k)
        return self._cache[k]

    def __setitem__(self, k, v):
        self._cache[k] = v

    def _get(self, k):
        raise NotImplementedError("Must be implemented by child class")

    def __repr__(self):
        placeholder = "<not evaluated>"
        return str({k: self._cache.get(k, placeholder) for k in self.keys()})
