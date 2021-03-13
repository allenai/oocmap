#ifndef OOCMAP_OOCMAP_H
#define OOCMAP_OOCMAP_H

#include <unordered_map>
#include <vector>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "lmdb.h"

extern PyTypeObject OOCMapType;

typedef struct {
    PyObject_HEAD
    MDB_env* mdb;
    MDB_dbi rootDb;
    MDB_dbi intsDb;
    MDB_dbi stringsDb;
    MDB_dbi listsDb;
    MDB_dbi tuplesDb;
    MDB_dbi dictsDb;
} OOCMapObject;

#pragma pack(push, 1)

struct ListKey {
    // The order of these matters, since this struct is taken together
    // as a uint64 and compared, and we need to make sure that adjacent
    // list items are compared adjacently and in the right order.
    // On a big-endian platform, these will likely have to be reversed.
    uint32_t listIndex;
    uint32_t listId;

    static const uint32_t listIndexLength;
};

struct DictKey {
    uint32_t dictId;
    uint32_t reserved;
};

struct EncodedValue {
    union {
        uint8_t asChars[8];
        int64_t asInt;
        uint64_t asUInt;
        double asFloat;
        ListKey asListKey;
        DictKey asDictKey;
    };
    union {
        struct {
            uint8_t typeCode: 5;
            uint8_t lengthMinusOne: 3;
        };
        uint8_t typeCodeWithLength;
    };
    // The maximum possible length we want to express is 8, but the length field has only 3 bits.
    // Since none of the types that use the length field can be of length 0, we store length-1
    // instead.

    bool operator==(const EncodedValue& other) const {
        return asUInt == other.asUInt && typeCodeWithLength == other.typeCodeWithLength;
    }

    bool operator!=(const EncodedValue& other) const {
        return asUInt != other.asUInt || typeCodeWithLength != other.typeCodeWithLength;
    }
};
_Static_assert(sizeof(EncodedValue) == 9, "EncodedValue must be 9 bytes in size.");

// This is the structure that defines the keys in the dicts table. Dicts are different
// because the keys are not integers but variable-length.
struct DictItemKey {
    uint32_t dictId;
    EncodedValue key;
};

#pragma pack(pop)


// This is seriously how to write a custom hash function, no joke 🙄
namespace std {
    template<> struct hash<EncodedValue> {
        size_t operator()(const EncodedValue& value) const {
            return static_cast<size_t>(value.asUInt) ^ (
                static_cast<size_t>(value.typeCodeWithLength) <<
                    ((sizeof(size_t) - sizeof(value.typeCodeWithLength)) * 8)
            );
        };
    };
}


// Mapping PyObjects to EncodedValues so we can avoid encoding the same value twice.
typedef std::unordered_map<PyObject*, EncodedValue> Id2EncodedMap;
// Mapping EncodedValues to PyObjects so we can avoid decoding the same value twice.
typedef std::unordered_map<EncodedValue, PyObject*> Encoded2IdMap;


struct OOCTransaction {
    bool readonly;
    bool txnOwned;
    MDB_txn* txn;
    Id2EncodedMap insertedItems;

    explicit OOCTransaction(OOCMapObject* ooc, bool readonly);
    explicit OOCTransaction(MDB_txn* txn, bool readonly);
    ~OOCTransaction();

    void commit();
    void abort();

private:
    void clear();
};

const EncodedValue* OOCMap_encode(OOCMapObject* self, PyObject* value, OOCTransaction& txn);
PyObject* OOCMap_decode(OOCMapObject* self, EncodedValue* encodedValue, OOCTransaction& txn);


const uint8_t TYPE_CODE_HARDCODED = 0;
const uint8_t TYPE_CODE_SHORT_POSITIVE_INT = 1;
const uint8_t TYPE_CODE_SHORT_NEGATIVE_INT = 2;
const uint8_t TYPE_CODE_LONG_POSITIVE_INT = 3;
const uint8_t TYPE_CODE_LONG_NEGATIVE_INT = 4;
const uint8_t TYPE_CODE_FLOAT = 5;
const uint8_t TYPE_CODE_UNICODE_SHORT_WCHAR = 6;
const uint8_t TYPE_CODE_UNICODE_SHORT_1BYTE = 7;
const uint8_t TYPE_CODE_UNICODE_SHORT_2BYTE = 8;
const uint8_t TYPE_CODE_UNICODE_SHORT_4BYTE = 9;
const uint8_t TYPE_CODE_UNICODE_LONG_WCHAR = 10; static const uint8_t TYPE_CODE_UNICODE_LONG_SHORT_OFFSET = TYPE_CODE_UNICODE_LONG_WCHAR - TYPE_CODE_UNICODE_SHORT_WCHAR;
const uint8_t TYPE_CODE_UNICODE_LONG_1BYTE = 11;
const uint8_t TYPE_CODE_UNICODE_LONG_2BYTE = 12;
const uint8_t TYPE_CODE_UNICODE_LONG_4BYTE = 13;
const uint8_t TYPE_CODE_TUPLE = 14;
const uint8_t TYPE_CODE_LIST = 15;
const uint8_t TYPE_CODE_DICT = 16;
const uint8_t TYPE_CODE_SET = 17;
const uint8_t TYPE_CODE_COMPLEX = 18;
const uint8_t TYPE_CODE_BYTES = 19;
const uint8_t TYPE_CODE_BYTEARRAY = 20;


#endif
