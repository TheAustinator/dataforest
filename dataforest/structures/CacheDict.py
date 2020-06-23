class CacheDict(dict):
    def __init__(self):
        self._cache = dict()
        super().__init__()

    def __getitem__(self, k):
        if k not in self._cache:
            self._cache[k] = super().__getitem__(k).read()
        return self._cache[k]
