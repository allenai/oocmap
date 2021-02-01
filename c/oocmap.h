#ifndef OOCMAP_OOCMAP_H
#define OOCMAP_OOCMAP_H

#include <unordered_map>

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

#pragma pack(1)

struct EncodedValue {
    union {
        uint8_t asChars[8];
        int64_t asInt;
        uint64_t asUInt;
        double asFloat;
        struct {
            uint32_t listId;
            uint32_t listIndex;
        } asListKey;
        struct {
            uint32_t dictId;
            uint32_t reserved;
        } asDictKey;
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

#pragma options align=reset

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
typedef std::unordered_map<PyObject*, const EncodedValue*> Id2EncodedMap;
// Mapping EncodedValues to PyObjects so we can avoid decoding the same value twice.
typedef std::unordered_map<EncodedValue, PyObject*> Encoded2IdMap;

void OOCMap_encode(
    OOCMapObject* self,
    PyObject* value,
    EncodedValue* dest,
    MDB_txn* txn,
    Id2EncodedMap& insertedItemsInThisTransaction,
    bool readonly = false
);
PyObject* OOCMap_decode(OOCMapObject* self, EncodedValue* encodedValue, MDB_txn* txn);

#endif
