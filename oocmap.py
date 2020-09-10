import codecs
import random
import struct
from contextlib import contextmanager
from hashlib import blake2s
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

def _random_bytes(n: int) -> bytes:
    return bytes(random.getrandbits(8) for _ in range(n))

class OOCMap(object):
    def __init__(self, filename: Union[str, PathLike], *, max_size: int = 1024*1024*1024*1024):
        self.lmdb_env = lmdb.open(
            filename,
            subdir=False,
            map_size=max_size,
            max_readers=os.cpu_count() * 2,
            max_spare_txns=os.cpu_count() * 2,
            max_dbs=6,
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
            b"lists",
            integerkey=True)
        self.tuples_db = self.lmdb_env.open_db(
            b"tuples",
            integerkey=True)
        self.dicts_db = self.lmdb_env.open_db(
            b"dicts",
            integerkey=False)

        self.id_to_key = {}
        self.ids_written_this_transaction = {}
        self.transaction_count = 0

    def begin_transaction(self):
        if self.transaction_count <= 0:
            self.ids_written_this_transaction.clear()
        self.transaction_count += 1

    def end_transaction(self):
        if self.transaction_count <= 0:
            raise ValueError("No transaction running.")
        self.transaction_count -= 1
        if self.transaction_count <= 0:
            self.ids_written_this_transaction.clear()

    @contextmanager
    def transaction(self):
        self.begin_transaction()
        try:
            yield self
        finally:
            self.end_transaction()

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

        already_written_encoding = self.ids_written_this_transaction.get(id(v))
        if already_written_encoding is not None:
            b.extend(already_written_encoding)
            return

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
            b.append(3)                                     # type code for floats
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
                self._encode(encoded, v2, write_to_db)
            h = self._write_immutable_value_to_db(encoded, self.tuples_db, write_to_db)
            b.append(LazyTuple.TYPE_CODE)                   # type code for non-empty tuple
            b.extend(h)
        elif isinstance(v, list):
            if not write_to_db:
                raise ValueError("Can't encode mutable values without writing to the db.")

            # Encode all the list items up front so we don't create a mega
            # transaction for a mega list.
            v_encoded = []
            for item in v:
                item_encoded = bytearray()
                self._encode(item_encoded, item, write_to_db)
                v_encoded.append(bytes(item_encoded))

            # Keys for list_db are half a unique identifier of the list, and half the index
            # of the element. The key where the index is 0xffffffff stores the length.
            key = self.id_to_key.get(id(v))
            with self.lmdb_env.begin(write=True, db=self.lists_db) as txn:
                if key is None:
                    key = _random_bytes(4) + b"\xff\xff\xff\xff"
                    while txn.get(key) is not None:
                        key = _random_bytes(4) + b"\xff\xff\xff\xff"
                txn.put(key, len(v).to_bytes(4, _BYTEORDER, signed=False))
                for i, item_encoded in enumerate(v_encoded):
                    item_key = key[:4] + i.to_bytes(4, _BYTEORDER, signed=False)
                    txn.put(item_key, item_encoded)

            self.id_to_key[id(v)] = key

            b.append(LazyList.TYPE_CODE)                # type code for list
            b.extend(key)
        elif isinstance(v, dict):
            if not write_to_db:
                raise ValueError("Can't encode mutable values without writing to the db.")

            # Encode keys and values up front so we don't create a mega
            # transaction for a mega dict.
            key_values_encoded = []
            for key, value in v.items():
                key_encoded = bytearray()
                self._encode(key_encoded, key, write_to_db)
                value_encoded = bytearray()
                self._encode(value_encoded, value, write_to_db)
                key_values_encoded.append((bytes(key_encoded), bytes(value_encoded)))

            key = self.id_to_key.get(id(v))
            with self.lmdb_env.begin(write=True, db=self.dicts_db) as txn:
                if key is None:
                    key = _random_bytes(4)
                    while txn.get(key) is not None:
                        key = _random_bytes(4)
                txn.put(key, len(v).to_bytes(4, _BYTEORDER, signed=False))
                for key_encoded, value_encoded in key_values_encoded:
                    txn.put(key + key_encoded, value_encoded)

            self.id_to_key[id(v)] = key

            b.append(LazyDict.TYPE_CODE)                # type code for dict
            b.extend(key + b"\x00\x00\x00\x00")
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
            return LazyDict(self, encoded[:4])
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
        with self.transaction():
            encoded_key = bytearray()
            self._encode(encoded_key, key)
            encoded_value = bytearray()
            self._encode(encoded_value, value)

            with self.lmdb_env.begin(write=True, db=self.root_db) as txn:
                txn.put(encoded_key, encoded_value)

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

    def __len__(self) -> int:
        with self.lmdb_env.begin(write=False) as txn:
            return txn.stat(self.root_db)["entries"]

class _Lazy:
    __slots__ = ["ooc", "key"]
    TYPE_CODE = NotImplemented

    def __init__(self, ooc: OOCMap, key: bytes):
        self.ooc = ooc
        self.key = bytes(key)

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

    def __len__(self) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.tuples_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack_from("<I", encoded)[0]
            return length

    def __getitem__(self, index) -> Any:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.tuples_db, buffers=True) as txn:
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

    def eager(self) -> Tuple:
        elements = []
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.tuples_db, buffers=True) as txn:
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
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.tuples_db, buffers=True) as txn:
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
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.tuples_db, buffers=True) as txn:
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
    # TODO: implement more List methods
    TYPE_CODE = 9

    def _key_for_index(self, index: int) -> bytes:
        return self.key[:4] + index.to_bytes(4, _BYTEORDER, signed=False)

    def __getitem__(self, index: int) -> Any:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            length = len(self)
            if index < length:
                index = length - index
            if index > length:
                raise IndexError("list index out of range")
            encoded = txn.get(self._key_for_index(index))
            return self.ooc._decode(encoded)

    def __setitem__(self, index: int, value):
        encoded = bytearray()
        self.ooc._encode(encoded, value)

        with self.ooc.lmdb_env.begin(write=True, db=self.ooc.lists_db) as txn:
            length = len(self)
            if index < length:
                index = length - index
            if index > length:
                raise IndexError("list index out of range")
            txn.put(self._key_for_index(index), encoded)

    def __len__(self) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack("<I", encoded)[0]
            return length

    def eager(self) -> List:
        elements = []
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            index = 0
            while True:
                encoded = txn.get(self._key_for_index(index))
                if encoded is None:
                    break
                elements.append(self.ooc._decode(encoded))
                index += 1
        return elements

    def __contains__(self, item) -> bool:
        if isinstance(item, _Lazy) and id(item.ooc) != id(self.ooc):
            return False
        return self.index(item) >= 0

    def count(self, item) -> int:
        c = 0
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            index = 0
            while True:
                encoded = txn.get(self._key_for_index(index))
                if encoded is None:
                    break
                if isinstance(item, _Lazy):
                    if item.TYPE_CODE == encoded[0] and item.key == encoded[1:]:
                        c += 1
                else:
                    element = self.ooc._decode(encoded)
                    if item == element:
                        c += 1
                index += 1
        return c

    def index(self, item, start: Optional[int] = None, end: Optional[int] = None) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.lists_db, buffers=True) as txn:
            if start is None:
                index = 0
            else:
                index = start
            while True:
                # This is some pretty weird indexing behavior, but that's what the built-in list does.
                if index < 0:
                    index = len(self) + index
                    if index < 0:
                        index = 0

                encoded = txn.get(self._key_for_index(index))
                if encoded is None:
                    break
                if isinstance(item, _Lazy):
                    if item.TYPE_CODE == encoded[0] and item.key == encoded[1:]:
                        return index
                else:
                    element = self.ooc._decode(encoded)
                    if item == element:
                        return index
                index += 1
                if end is not None and end >= index:
                    break
        raise ValueError(f"{item} is not in list")

    def clear(self) -> None:
        with self.ooc.lmdb_env.begin(write=True, db=self.ooc.lists_db) as txn:
            index = 0
            while True:
                deleted = txn.delete(self._key_for_index(index))
                if not deleted:
                    break
            txn.put(self.key, int(0).to_bytes(4, _BYTEORDER, signed=False))

    def append(self, value):
        # first write the new value into the map
        encoded = bytearray()
        self.ooc._encode(encoded, value)

        with self.ooc.lmdb_env.begin(write=True, db=self.ooc.lists_db) as txn:
            index = len(self)
            txn.put(self._key_for_index(index), encoded)
            txn.put(self.key, (index+1).to_bytes(4, _BYTEORDER, signed=False))


class LazyDict(_Lazy):
    # TODO: implement more dict methods
    TYPE_CODE = 11

    def __len__(self) -> int:
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.dicts_db, buffers=True) as txn:
            encoded = txn.get(self.key)
            length = struct.unpack("<I", encoded)[0]
            return length

    def __getitem__(self, key):
        key_encoded = bytearray(self.key)
        self.ooc._encode(key_encoded, key, write_to_db=False)
        with self.ooc.lmdb_env.begin(write=False, db=self.ooc.dicts_db, buffers=True) as txn:
            value_encoded = txn.get(key_encoded)
            if value_encoded is None:
                raise KeyError()
            return self.ooc._decode(value_encoded)

    def eager(self) -> Dict:
        result = {}
        with self.ooc.lmdb_env.begin(write=False, buffers=True) as txn:
            with txn.cursor(self.ooc.dicts_db) as cur:
                success = cur.set_key(self.key)
                assert success
                while cur.next():
                    key_encoded, value_encoded = cur.item()
                    if key_encoded[:len(self.key)] != self.key:
                        break
                    key_encoded = key_encoded[len(self.key):]
                    result[self.ooc._decode(key_encoded)] = self.ooc._decode(value_encoded)
        return result
