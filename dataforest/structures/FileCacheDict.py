from dataforest.structures import CacheDict


class FileCacheDict(CacheDict):
    def exists(self, k):
        try:
            _ = self[k]
            return True
        except FileNotFoundError:
            return False
