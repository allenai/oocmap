import tempfile

import pytest

from oocmap import OOCMap


SMALL_MAP = 32*1024*1024

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
        m = OOCMap(f.name, max_size=SMALL_MAP)
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
        m = OOCMap(f.name, max_size=SMALL_MAP)
        m[999] = ("Paul", "Ringo", "George", "John Winston Ono Lennon")

        l = [1, 2.0, "three", m[999]]
        m[0] = l
        assert l == m[0]
        assert m[0] == l  # not the same thing

        empty_l = []
        m[1] = empty_l
        assert empty_l == m[1]
        assert m[1] == empty_l

        # LazyList.__len__()
        assert len(l) == len(m[0])
        assert len(empty_l) == len(m[1])

        # Iterators
        assert l == list(iter(m[0]))
        assert empty_l == list(iter(m[1]))

        # LazyList.index()
        for item in l + ["notfound"]:
            # no indices
            assert_equal_including_exceptions(
                lambda: l.index(item),
                lambda: m[0].index(item))

            # only start index
            for index in range(-10, 10):
                assert_equal_including_exceptions(
                    lambda: l.index(item, index),
                    lambda: m[0].index(item, index))

            # start and stop index
            for start_index in range(-10, 10):
                for end_index in range(-10, 10):
                    assert_equal_including_exceptions(
                        lambda: l.index(item, start_index, end_index),
                        lambda: m[0].index(item, start_index, end_index))

        # LazyList.__getitem__()
        for index in range(-10, 10):
            assert_equal_including_exceptions(
                lambda: l[index],
                lambda: m[0][index])

        # LazyList.count()
        for item in l + ["notfound"]:
            assert_equal_including_exceptions(
                lambda: l.count(item),
                lambda: m[0].count(item))

        # LazyList.__contains__()
        for item in l + ["notfound"]:
            assert_equal_including_exceptions(
                lambda: item in l,
                lambda: item in m[0])

        # LazyList.append()
        m[0].append(4)
        assert m[0].eager() == [1, 2.0, "three", m[999], 4]

        # LazyList.extend()
        m[1].extend(empty_l)
        assert m[1].eager() == []
        m[1].extend(l)
        assert m[1].eager() == [1, 2.0, "three", m[999]]
        m[1].extend(m[0])
        assert m[1].eager() == [1, 2.0, "three", m[999], 1, 2.0, "three", m[999], 4]

        # LazyList.__del__()
        del m[0][-1]  # make the two lists the same again after append
        assert m[0].eager() == l
        assert l == m[0].eager()
        for index in [-10, 10, -2, 1]:
            def delete_l():
                del l[index]
            def delete_m():
                del m[0][index]
            assert_equal_including_exceptions(delete_l, delete_m)
        assert l == m[0].eager()

        # LazyList.__setitem__()
        for index in range(-5, 5):
            def assign_l():
                l[index] = None
            def assign_m():
                m[0][index] = None
            assert_equal_including_exceptions(assign_l, assign_m)
        assert l == [None] * 2
        assert l == m[0].eager()

        # LazyList.clear()
        m[0].clear()
        assert m[0].eager() == []

def test_oocmap_tuple():
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=SMALL_MAP)
        m[999] = ("Paul", "Ringo", "George", "John Winston Ono Lennon")

        t = (1, 2.0, "three", m[999])
        m[0] = t
        assert t == m[0]
        assert m[0] == t # not the same thing

        # LazyTuple.__len__()
        assert len(t) == len(m[0])

        # LazyTuple.index()
        for item in list(t) + ["notfound"]:
            # no indices
            assert_equal_including_exceptions(
                lambda: t.index(item),
                lambda: m[0].index(item))

            # only start index
            for index in range(-10, 10):
                assert_equal_including_exceptions(
                    lambda: t.index(item, index),
                    lambda: m[0].index(item, index))

            # start and stop index
            for start_index in range(-10, 10):
                for end_index in range(-10, 10):
                    assert_equal_including_exceptions(
                        lambda: t.index(item, start_index, end_index),
                        lambda: m[0].index(item, start_index, end_index))

        # LazyTuple.__getitem__()
        for index in range(-10, 10):
            assert_equal_including_exceptions(
                lambda: t[index],
                lambda: m[0][index])

        # LazyTuple.count()
        for item in list(t) + ["notfound"]:
            assert_equal_including_exceptions(
                lambda: t.count(item),
                lambda: m[0].count(item))

        # LazyTuple.__contains__()
        for item in list(t) + ["notfound"]:
            assert_equal_including_exceptions(
                lambda: item in t,
                lambda: item in m[0])

        # LazyTuple.__add__()
        assert m[999] + ("Yoko",) == ("Paul", "Ringo", "George", "John Winston Ono Lennon", "Yoko")
        assert ("Yoko",) + m[999] == ("Yoko", "Paul", "Ringo", "George", "John Winston Ono Lennon")
        assert m[999] + m[0] == ("Paul", "Ringo", "George", "John Winston Ono Lennon") + t
        assert m[0] + m[999] == t + ("Paul", "Ringo", "George", "John Winston Ono Lennon")


def test_oocmap_dict():
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=SMALL_MAP)
        m[999] = ("Paul", "Ringo", "George", "John Winston Ono Lennon")

        d = {
            1: "uno",
            "two": (2, "zwei", [2, 2]),
            (3, 3, "drei"): {
                "north": ("u", "p"),
                "east": ["right"],
                "south": "I come from a land down under!",
                "west": m[999]
            },
            m[999]: "beatles",
            (1,): "null",
            (1,(1,)): "bull",
            (1,(2,)): "troll",
            (1,(m[999],)): "quell"
        }
        m[0] = d
        assert m[0] == d
        assert d == m[0]

        # LazyDict.__len__()
        assert len(m[0]) == len(d)

        # LazyDict.__getitem__()
        for key in list(d.keys()) + ["notfound", [1,2,3]]:
            assert_equal_including_exceptions(
                lambda: d[key],
                lambda: m[0][key])

        # LazyDict,__setitem__()
        for key in [2, "three", [1,2,3], 1]:
            def assign_d():
                d[key] = None
            def assign_m():
                m[0][key] = None
            assert_equal_including_exceptions(assign_d, assign_m)
        assert len(m[0]) == len(d)

        # LazyDict.__getitem__() again
        for key in list(d.keys()) + ["notfound", [1,2,3]]:
            assert_equal_including_exceptions(
                lambda: d[key],
                lambda: m[0][key])

        # LazyDict.__delitem__()
        for key in [2, "three", [1,2,3], 1]:
            def delitem_d():
                del d[key]
            def delitem_m():
                del m[0][key]
            assert_equal_including_exceptions(delitem_d, delitem_m)
        assert len(m[0]) == len(d)

        # LazyDict.__getitem__() again
        for key in list(d.keys()) + ["notfound", [1,2,3]]:
            assert_equal_including_exceptions(
                lambda: d[key],
                lambda: m[0][key])

        # LazyDict.__contains__()
        for item in list(d.keys()) + ["notfound", (1, [1,2,3])]:
            assert_equal_including_exceptions(
                lambda: item in d,
                lambda: item in m[0])

        # LazyDict.__iter__()
        for key in d:
            assert key in m[0]
        for key in m[0]:
            assert key in d

        # LazyDict.keys()
        for key in d.keys():
            assert key in m[0].keys()
        for key in m[0].keys():
            assert key in d.keys()

        # LazyDict.values()
        for value in d.values():
            assert value in m[0].values()
        for value in m[0].values():
            assert value in d.values()

        # LazyDict.items()
        for item in d.items():
            assert item in m[0].items()
        for item in m[0].items():
            assert item in d.items()


def test_two_oocmaps():
    with tempfile.NamedTemporaryFile() as f1:
        m1 = OOCMap(f1.name, max_size=SMALL_MAP)
        with tempfile.NamedTemporaryFile() as f2:
            m2 = OOCMap(f2.name, max_size=SMALL_MAP)

            m1[1033] = ["one", "two", "three"]
            m1[1031] = ["eins", "zwei", "drei"]
            m1[1041] = ["一", "二", "三"]
            m2[0] = [m1[1033], m1[1031], m1[1041]]
            assert m2[0] == [["one", "two", "three"], ["eins", "zwei", "drei"], ["一", "二", "三"]]



