import pytest

from dataforest.filesystem.DataTree import DataTree


@pytest.fixture
def dt_from_str():
    test_str = "normalized:disease:disease_1+disease_2-protocol:A+C--!disease:disease_2-state:healthy--!experiment:expt_4-@disease"
    dt = DataTree.from_str(test_str)
    dt_dict = dt.dict
    return dt_dict


def test_from_str(dt_from_str):
    expected = {
        "subset": [{"normalized": {"disease": {"disease_1", "disease_2"}, "protocol": {"A", "C"},}}],
        "filter": [{"disease": "disease_2", "state": "healthy"}, {"experiment": "expt_4"},],
        "group": [{"disease",}],
    }
    # This fails because of a some dict comparison problem with pytest
    assert dt_from_str == expected
