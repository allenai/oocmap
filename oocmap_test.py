import tempfile

import pytest

from oocmap import OOCMap


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

def test_oocmap_list_append():
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=32*1024*1024)
        l = [1,2,3]
        m[0] = l
        l_from_map = m[0]
        assert l == l_from_map
        l_from_map.append(4)
        assert m[0].eager() == [1,2,3,4]
