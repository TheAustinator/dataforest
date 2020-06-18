import pytest

from dataforest.Tree import Tree


@pytest.fixture
def test_branch_1():
    t = Tree()
    t.dict["a"] = None
    t.down("a")
    t[t.stack] = "b"
    t.down("b")
    t[t.stack] = "c"
    t.down("c")
    t.up()
    t.up()
    t[t.stack] = "c"
    t.down("c")
    t[t.stack] = "d"
    print(t.dict)
    assert t.dict == {"a": {"b": "c", "c": "d"}}
    return t


@pytest.fixture
def test_branch_2():
    t = Tree()
    t.dict["a"] = None
    t.down("a")
    t[t.stack] = "b"
    t[t.stack] = "c"
    t.down("c")
    t[t.stack] = "d"
    t[t.stack] = "e"
    print(t.dict)
    assert t.dict == {"a": {"b": None, "c": {"d", "e"}}}
    return t


def test_node_awareness_1(test_branch_1):
    t = test_branch_1
    t.stack = []
    assert t.at_root and (not t.at_leaf)
    t.down("a")
    assert (not t.at_root) and (not t.at_leaf)
    t.down("b")
    assert not t.at_leaf
    t.down()
    assert t.at_leaf
    t.up()
    assert not t.at_leaf


def test_node_awareness_2(test_branch_2):
    t = test_branch_2
    t.stack = []
    t.down("a")
    t.down("c")
    assert not t.at_leaf
    t.down("d")
    assert t.at_leaf


def test_apply_leaves_1(test_branch_1):
    t = test_branch_1
    t_2 = t.apply_leaves(lambda x: 1)
    print(t_2.dict)


def test_apply_leaves_2(test_branch_2):
    t = test_branch_2
    t_2 = t.apply_leaves(lambda x: 1)
    print(t_2.dict)
