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
        "dict": {
            1: "eins",
            2: "zwei"
        }
    }
    with tempfile.NamedTemporaryFile() as f:
        m = OOCMap(f.name, max_size=32*1024*1024)
        for key, value in tests.items():
            m[key] = value
        for key, value in tests.items():
            retrieved = m[key]
            assert retrieved == value
            del m[key]
            with pytest.raises(KeyError):
                _ = m[key]
        assert len(m) == 0
