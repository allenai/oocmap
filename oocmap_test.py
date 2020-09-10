import tempfile

import pytest

from oocmap import OOCMap


def result_or_exception(fn):
    try:
        return fn()
    except Exception as e:
        return e


def assert_equal_including_exceptions(expected_fn, actual_fn):
    try:
        expected = expected_fn()
    except Exception as e:
        with pytest.raises(e.__class__):
            actual_fn()
    else:
        assert expected == actual_fn()


def test_oocmap_types():
    tests = {
        "smallint": 42,
        "largeint": 162259276829213363391578010288127,
        "float": 1/3,
        "smallstr": "ok",
        "longstr": "Wer lesen kann ist klar im Vorteil.",
        "8str": "12345678",
        "bool": True,
        "none": None,
        "emptytuple": (),
        "tuple": (1, True, False, 0, 2, 3),
        "list": [2, 3],
        "dict": { 1: "eins", 2: "zwei" }
    }
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=32*1024*1024)
        for key, value in tests.items():
            m[key] = value
        assert len(m) == len(tests)
        for key, value in tests.items():
            retrieved = m[key]
            assert retrieved == value
            del m[key]
            with pytest.raises(KeyError):
                _ = m[key]
        assert len(m) == 0

def test_oocmap_list():
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=32*1024*1024)
        m[999] = ("Paul", "Ringo", "George", "John Winston Ono Lennon")

        l = [1, 2.0, "three", m[999]]
        m[0] = l
        assert l == m[0]

        assert m[0].index(2) == 1
        with pytest.raises(ValueError):
            m[0].index(5)
        for item in l + ["notfound"]:
            for index in range(-10, 10):
                assert_equal_including_exceptions(
                    lambda: l.index(item, index),
                    lambda: m[0].index(item, index))

        for item in l + ["notfound"]:
            assert_equal_including_exceptions(
                lambda: l.count(item),
                lambda: m[0].count(item))

        m[0].append(4)
        assert m[0].eager() == [1, 2.0, "three", m[999], 4]

        m[0].clear()
        assert m[0].eager() == []

        m[0] = l


