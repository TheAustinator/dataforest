from dataforest.utils import tether


class TetherTester:
    def __init__(self, val):
        self.val = val

    @staticmethod
    def my_method(val):
        return val + 5


def test_tether():
    obj = TetherTester(2)
    tether(obj, "val")
    assert obj.my_method() == 7


if __name__ == "__main__":
    test_tether()
