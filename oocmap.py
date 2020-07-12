import codecs
import struct
from hashlib import blake2b, blake2s
from os import PathLike
from typing import *

import lmdb
import os

_BYTEORDER = 'little'

_HARDCODED_VALUES = [
    None,
    True,
    False,
    ()
    # TODO: Add other things we might want to hard-code. Probably as many Python primitives as we can think of.
]

# Preprocessing of _HARDCODED_VALUES

# Because Python dictionaries are insane and 1 == True as far as dictionary lookup is concerned,
# we keep the original type with the value.
_HARDCODED_VALUES = {
    (type(v), v): i.to_bytes(length=8, byteorder=_BYTEORDER, signed=False)
    for i, v in enumerate(_HARDCODED_VALUES)
}

_HARDCODED_VALUES_REVERSE = {
    c: v for (t, v), c in _HARDCODED_VALUES.items()
}

# Assert that there are no duplicates.
assert len(_HARDCODED_VALUES_REVERSE) == len(_HARDCODED_VALUES)

def _hardcoded_encoding(v: Any) -> Optional[bytes]:
    try:
        return _HARDCODED_VALUES.get((type(v), v))
    except TypeError:   # unhashable types raise this
        return None
def _hardcoded_decoding(b: bytes) -> Any:
    return _HARDCODED_VALUES_REVERSE[b]

class OOCMap(object):
    def __init__(self, filename: Union[str, PathLike], *, max_size: int = 1024*1024*1024*1024):
        self.lmdb_env = lmdb.open(
            filename,
            subdir=False,
            map_size=max_size,
            max_readers=os.cpu_count() * 2,
            max_spare_txns=os.cpu_count() * 2,
            max_dbs=5,
            writemap=False,
            metasync=False,
            sync=True,
            meminit=False,
            map_async=False)
        self.root_db = self.lmdb_env.open_db(
            b"root",
            integerkey=False)
        self.ints_db = self.lmdb_env.open_db(
            b"ints",
            integerkey=True)
        self.strings_db = self.lmdb_env.open_db(
            b"strings",
            integerkey=True)
        self.lists_db = self.lmdb_env.open_db(
            b"lists_and_tuples",
            integerkey=True)
        self.dicts_db = self.lmdb_env.open_db(
            b"dicts",
            integerkey=True)

        # We use a random salt to salt keys for mutable objects.
        import random
        self.salt = random.randrange(2**63)

        self.id_to_key = {}

    def _key_for_mutable_value(self, value: Any) -> bytes:
        return (id(value) ^ self.salt).to_bytes(8, _BYTEORDER, signed=False)

    def _write_mutable_value_to_db(
        self,
        key: Union[bytes, bytearray],
        encoded: Union[bytes, bytearray],
        db
    ) -> None:
        assert len(key) == 8
        with self.lmdb_env.begin(write=True, db=db) as txn:
            txn.put(key, encoded)

    def _write_immutable_value_to_db(
        self,
        encoded: Union[bytes, bytearray],
        db,
        write_to_db: bool = True
    ) -> bytes:
        encoded_hash = blake2s(encoded).digest()[:8]
        if write_to_db:
            with self.lmdb_env.begin(write=True, db=db) as txn:
                new_item = txn.put(encoded_hash, encoded, overwrite=False)
                assert new_item, "hash collision?"
        return encoded_hash

    def _encode(self, b: bytearray, v: Any, write_to_db: bool = True) -> None:
        """Encodes the value v into the bytearray b.
        This will always add exactly 9 bytes to b."""

        length_before = len(b)

        hardcoded = _hardcoded_encoding(v)
        if hardcoded is not None:
            b.append(0)
            b.extend(hardcoded)
        elif isinstance(v, int):
            try:
                encoded = v.to_bytes(8, _BYTEORDER, signed=True)
                b.append(1)                                 # type code for 8-byte ints
                b.extend(encoded)
            except OverflowError:
                length = 9
                encoded = None
                while True:
                    try:
                        encoded = v.to_bytes(length, _BYTEORDER, signed=True)
                        break
                    except OverflowError:
                        length += 1
                        continue
                h = self._write_immutable_value_to_db(encoded, self.ints_db, write_to_db)
                b.append(2)                                 # type code for longer ints
                b.extend(h)
        elif isinstance(v, float):
            b.append(3)                                     # type code for shorts
            b.extend(struct.pack("<d", v))
        elif isinstance(v, str):
            encoded = codecs.encode(v)
            if len(encoded) < 9:
                b.append(4)                                 # type code for short strings
                b.extend(encoded)
                b.extend(0 for _ in range(9 - 1 - len(encoded)))
            else:
                h = self._write_immutable_value_to_db(encoded, self.strings_db, write_to_db)
                b.append(5)                                 # type code for long strings
                b.extend(h)
        elif isinstance(v, tuple):
            encoded = bytearray()
            encoded.extend(struct.pack("<I", len(v)))
            for v2 in v:
                self._encode(encoded, v2)
            h = self._write_immutable_value_to_db(encoded, self.lists_db, write_to_db)
            b.append(LazyTuple.TYPE_CODE)                   # type code for non-empty tuple
            b.extend(h)
        elif isinstance(v, list):
            if not write_to_db:
                raise ValueError("Can't encode mutable values without writing to the db.")
            key = self.id_to_key.get(id(v))
            if key is not None:
                assert len(key) == 8
                b.append(LazyList.TYPE_CODE)                # type code for list
                b.extend(key)
            else:
                encoded = bytearray()
                encoded.extend(struct.pack("<I", len(v)))
                for v2 in v:
                    self._encode(encoded, v2)
                key = self._key_for_mutable_value(v)
                self._write_mutable_value_to_db(key, encoded, self.lists_db)
                self.id_to_key[id(v)] = key
                b.append(LazyList.TYPE_CODE)                # type code for list
                b.extend(key)
        elif isinstance(v, dict):
            if not write_to_db:
                raise ValueError("Can't encode mutable values without writing to the db.")
            key = self.id_to_key.get(id(v))
            if key is not None:
                assert len(key) == 8
                b.append(LazyDict.TYPE_CODE)                # type code for dict
                b.extend(key)
            else:
                # We store first all keys, then all values. We do this so when we're reading it we can
                # read in all the keys at once for fast lookup.
                encoded = bytearray()
                encoded.extend(struct.pack("<I", len(v)))
                encoded_values = bytearray()
                for k2, v2 in v.items():
                    self._encode(encoded, k2)
                    self._encode(encoded_values, v2)
                encoded.extend(encoded_values)
                key = self._key_for_mutable_value(v)
                self._write_mutable_value_to_db(key, encoded, self.dicts_db)
                self.id_to_key[id(v)] = key
                b.append(LazyDict.TYPE_CODE)                # type code for dict
                b.extend(key)
        else:
            raise NotImplementedError("This type is not supported.")

        assert len(b) == length_before + 9

    def _decode(self, b: Union[bytes, bytearray]):
        type_code = b[0]
        encoded = b[1:9]
        if type_code == 0:
            # hardcoded values
            # This also captures the case where the return value is None.
            return _hardcoded_decoding(encoded)
        elif type_code == 1:
            # normal int
            return int.from_bytes(encoded, _BYTEORDER, signed=True)
        elif type_code == 2:
            # big int
            with self.lmdb_env.begin(write=False, db=self.ints_db) as txn:
                r = txn.get(encoded)
            return int.from_bytes(r, _BYTEORDER)
        elif type_code == 3:
            # float/double
            return struct.unpack("<d", encoded)[0]
        elif type_code == 4:
            # short string
            for first_zero in range(len(encoded)):
                if encoded[first_zero] == 0:
                    return codecs.decode(encoded[:first_zero])
            return codecs.decode(encoded)
        elif type_code == 5:
            # long string
            with self.lmdb_env.begin(write=False, db=self.strings_db) as txn:
                r = txn.get(encoded)
            return codecs.decode(r)
        elif type_code == LazyList.TYPE_CODE:
            return LazyList(self, encoded)
        elif type_code == LazyTuple.TYPE_CODE:
            return LazyTuple(self, encoded)
        elif type_code == LazyDict.TYPE_CODE:
            return LazyDict(self, encoded)
        else:
            raise ValueError(f"Unknown type code {type_code}")

    def __del__(self):
        # TODO: Figure out if this gets called on Ctrl-C
        if self.lmdb_env is not None:
            self.lmdb_env.close()
            self.lmdb_env = None

    def __setitem__(self, key, value):
        # If the key has mutable elements in it, and those elements change,
        # the lookup will fail later.

        # We clear this before we start inserting, because the next
        # time we get called the mutable objects might have been mutated,
        # and we need to make sure they get written out again.
        self.id_to_key.clear()

        encoded_key = bytearray()
        self._encode(encoded_key, key)
        encoded_value = bytearray()
        self._encode(encoded_value, value)

        with self.lmdb_env.begin(write=True, db=self.root_db) as txn:
            txn.put(encoded_key, encoded_value)

        # We clear it again just so we don't carry this dict around for no reason.
        self.id_to_key.clear()

    def __getitem__(self, key):
        encoded_key = bytearray()
        self._encode(encoded_key, key, write_to_db=False)
        with self.lmdb_env.begin(write=False, db=self.root_db, buffers=True) as txn:
            encoded_value = txn.get(encoded_key)
            if encoded_value is None:
                raise KeyError()
            value = self._decode(encoded_value)
        return value

    def __delitem__(self, key):
        encoded_key = bytearray()
        self._encode(encoded_key, key, write_to_db=False)
        with self.lmdb_env.begin(write=True, db=self.root_db) as txn:
            deleted = txn.delete(encoded_key)
            # If this pointed to any other objects, they are now orphaned and
            # will not be garbage collected.
        if not deleted:
            raise KeyError()


class _Lazy:
    __slots__ = ["ooc", "key"]
    TYPE_CODE = NotImplemented

    def __init__(self, ooc: OOCMap, key: bytes):
        self.ooc = ooc
        self.key = key

    def __repr__(self):
        return f"{self.__class__.__name__}(...)"

    def __str__(self):
        return self.eager().__str__()

    def eager(self):
        raise NotImplementedError()

    def _lazy_equal(self, other) -> bool:
        if id(other) == id(self):
            return True
        if isinstance(other, _Lazy) and id(other.ooc) == id(self.ooc) and other.key == self.key:
            return True
        return False

    def __add__(self, other):
        return self.eager().__add__(other)

    def __contains__(self, item) -> bool:
        return self.eager().__contains__(item)

    def __eq__(self, other) -> bool:
        if id(other) == id(self):
            return True
        if isinstance(other, _Lazy):
            return id(other.ooc) == id(self.ooc) and other.key == self.key
        return self.eager().__eq__(other)

    def __ge__(self, other) -> bool:
        if self._lazy_equal(other):
            return True
        return self.eager().__ge__(other)

    def __gt__(self, other) -> bool:
        if self._lazy_equal(other):
            return False
        return self.eager().__gt__(other)

    def __le__(self, other) -> bool:
        if self._lazy_equal(other):
            return True
        return self.eager().__le__(other)

    def __lt__(self, other) -> bool:
        if self._lazy_equal(other):
            return False
        return self.eager().__lt__(other)

    def __hash__(self):
        return hash((self.ooc, self.key))

    def __mul__(self, other):
        return self.eager().__mul__(other)

    def __rmul__(self, other):
        return self.eager().__rmul__(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __reduce__(self):
        return self.eager().__reduce__()

    def __reduce_ex__(self, protocol):
        return self.eager().__reduce_ex__(protocol)


class LazyTuple(_Lazy):
    TYPE_CODE = 7

    def __getitem__(self, index) -> Any:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            if index < length:
                index = length - index
            if index > length:
                raise IndexError()
            encoded = encoded[
                4 + index * 9,
                4 + index * 9 + 9
            ]
            return self.ooc._decode(encoded)

    def __len__(self) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            return length

    def eager(self) -> Tuple:
        elements = []
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            for index in range(length):
                encoded_item = encoded[
                    4 + index * 9:
                    4 + index * 9 + 9
                ]
                elements.append(self.ooc._decode(encoded_item))
        return tuple(elements)

    def __contains__(self, item) -> bool:
        if isinstance(item, _Lazy) and id(item.ooc) != id(self.ooc):
            return False
        return self.index(item) >= 0

    def count(self, item) -> int:
        c = 0
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            for index in range(length):
                encoded_item = encoded[
                    4 + index * 9:
                    4 + index * 9 + 9
                ]
                if isinstance(item, _Lazy):
                    if item.TYPE_CODE == encoded_item[0] and item.key == encoded_item[1:]:
                        c += 1
                else:
                    element = self.ooc._decode(encoded_item)
                    if item == element:
                        c += 1
        return c

    def index(self, item) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            for index in range(length):
                encoded_item = encoded[
                    4 + index * 9:
                    4 + index * 9 + 9
                ]
                if isinstance(item, _Lazy):
                    if item.TYPE_CODE == encoded_item[0] and item.key == encoded_item[1:]:
                        return index
                else:
                    element = self.ooc._decode(encoded_item)
                    if item == element:
                        return index
        return -1


class LazyList(_Lazy):
    TYPE_CODE = 9


class LazyDict(_Lazy):
    TYPE_CODE = 11


# old stuff

class LazyInt(_Lazy):
    def __int__(self) -> int:
        if self.value is None:
            with self.ooc.lmdb_env.begin(write=False, db=self.ooc.ints_db) as txn:
                r = txn.get(self.key)
            self.value = int.from_bytes(r, _BYTEORDER)
            self.ooc = None     # No need to keep the reference now
        return self.value
    __index__ = __int__
    def __float__(self):
        return int(self).__float__()

    def __add__(self, other):
        return int(self).__add__(other)
    def __sub__(self, other):
        return int(self).__sub__(other)
    def __mul__(self, other):
        return int(self).__mul__(other)
    def __truediv__(self, other):
        return int(self).__truediv__(other)
    def __floordiv__(self, other):
        return int(self).__floordiv__(other)
    def __mod__(self, other):
        return int(self).__mod__(other)
    def __divmod__(self, other):
        return int(self).__divmod__(other)
    def __pow__(self, other, modulo):
        return int(self).__pow__(other, modulo)
    def __lshift__(self, other):
        return int(self).__lshift__(other)
    def __rshift__(self, other):
        return int(self).__rshift__(other)
    def __and__(self, other):
        return int(self).__and__(other)
    def __xor__(self, other):
        return int(self).__xor__(other)
    def __or__(self, other):
        return int(self).__or__(other)

    def __radd__(self, other):
        return int(self).__radd__(other)
    def __rsub__(self, other):
        return int(self).__rsub__(other)
    def __rmul__(self, other):
        return int(self).__rmul__(other)
    def __rtruediv__(self, other):
        return int(self).__rtruediv__(other)
    def __rfloordiv__(self, other):
        return int(self).__rfloordiv__(other)
    def __rmod__(self, other):
        return int(self).__rmod__(other)
    def __rdivmod__(self, other):
        return int(self).__rdivmod__(other)
    def __rpow__(self, other, modulo):
        return int(self).__rpow__(other, modulo)
    def __rlshift__(self, other):
        return int(self).__rlshift__(other)
    def __rrshift__(self, other):
        return int(self).__rrshift__(other)
    def __rand__(self, other):
        return int(self).__rand__(other)
    def __rxor__(self, other):
        return int(self).__rxor__(other)
    def __ror__(self, other):
        return int(self).__ror__(other)

    def __neg__(self):
        return int(self).__neg__()
    def __pos__(self):
        return self
    def __abs__(self):
        return int(self).__abs__()
    def __invert__(self):
        return int(self).__invert__()

    def __round__(self, ndigits):
        return int(self).__round__(ndigits)
    def __trunc__(self):
        return self
    __floor__ = __trunc__
    __ceil__ = __trunc__

    def __str__(self):
        return int(self).__str__()


class LazyStr(_Lazy):
    __slots__ = ["ooc", "key", "value"]

    def __str__(self) -> str:
        if self.value is None:
            with self.ooc.lmdb_env.begin(write=False, db=self.ooc.strings_db) as txn:
                r = txn.get(self.key)
            self.value = r.decode("UTF-8")
            self.ooc = None     # No need to keep the reference now
        return self.value

