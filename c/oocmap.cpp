#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <unordered_map>
#include <memory>
#include <random>
#include "lmdb.h"
#include "spooky.h"


static std::mt19937 random_engine(std::chrono::system_clock::now().time_since_epoch().count());

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
    uint8_t typeCode : 5;
    uint8_t length : 3;
};
_Static_assert(sizeof(EncodedValue) == 9, "EncodedValue must be 9 bytes in size.");

// This is the structure that defines the keys in the dicts table. Dicts are different
// because the keys are not integers but variable-length.
struct DictItemKey {
    uint32_t dictId;
    EncodedValue key;
};

#pragma options align=reset

typedef std::unordered_map<PyObject*, const EncodedValue*> Id2EncodedMap;

struct OocError : std::exception {
    const enum ErrorCode {
        NoError,
        ImmutableValueNotFound,
        InvalidBool,
        CouldNotReadyString,
        InvalidStringKind,
        OutOfMemory,
        UnknownType,
        MdbError
    } errorCode;

    explicit OocError(const ErrorCode errorCode) : errorCode(errorCode) { }

    virtual void pythonize() const {
        switch(errorCode) {
        case NoError:
            PyErr_Format(PyExc_ValueError, "Error: There is no error.");
            break;
        case ImmutableValueNotFound:
            PyErr_Format(PyExc_ValueError, "Tried to write a non-existant immutable value into the DB in a readonly transaction.");
            break;
        case InvalidBool:
            PyErr_Format(PyExc_ValueError, "Found a bool that's neither true nor false.");
            break;
        case CouldNotReadyString:
            PyErr_Format(PyExc_MemoryError, "Could not bring string into the canonical representation.");
            break;
        case InvalidStringKind:
            PyErr_Format(PyExc_ValueError, "Unknown kind of string");
            break;
        case OutOfMemory:
            PyErr_NoMemory();
            break;
        case UnknownType:
            PyErr_Format(PyExc_ValueError, "Tried to serialize object of unknown type");
            break;
        case MdbError:
            PyErr_Format(PyExc_IOError, "Unknown problem with LMDB");
            break;
        }
    }
};

struct UnknownTypeError : OocError {
    PyObject* const type;

    explicit UnknownTypeError(PyObject* const type) :
        OocError(OocError::UnknownType),
        type(type)
    { }

    virtual void pythonize() const {
        PyErr_Format(
            PyExc_ValueError,
            "Cannot serialize objects of type %s",
            PyUnicode_AsUTF8(PyObject_Repr(type)));
    }
};

struct MdbError : OocError {
    const int mdbErrorCode;

    explicit MdbError(const int mdbErrorCode) :
        OocError(OocError::MdbError),
        mdbErrorCode(errorCode)
    { }

    virtual void pythonize() const {
        switch(mdbErrorCode) {
        case 0:
            PyErr_Format(PyExc_ValueError, "Error: There is no error.");
            break;
        case ENOMEM:
            PyErr_NoMemory();
            break;
        case EINVAL:
            PyErr_Format(PyExc_IOError,"LMDB: An invalid parameter was specified.");
            break;
        case ENOSPC:
            PyErr_Format(PyExc_IOError,"LMDB: No more disk space.");
            break;
        case EIO:
            PyErr_Format(PyExc_IOError,"LMDB: A low-level I/O error occurred while writing.");
            break;
        case EACCES:
            PyErr_Format(PyExc_IOError, "LMDB: Access denied");
            break;
        case ENOENT:
            PyErr_Format(PyExc_IOError, "LMDB Error: The directory specified by the path parameter doesn't exist.");
            break;
        case EAGAIN:
            PyErr_Format(PyExc_IOError, "LMDB Error: The environment was locked by another process.");
            break;
        default:
            PyErr_Format(PyExc_IOError, "MDB Error: %s", mdb_strerror(mdbErrorCode));
            break;
        }
    }
};


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


//
// Functions that are not exposed to Python
// These are allowed to throw exceptions.
//

static MDB_txn* txn_begin(OOCMapObject* const self, const bool write = false) {
    const unsigned int flags = write ? 0 : MDB_RDONLY;
    MDB_txn* txn = nullptr;
    int mapsizePatience = 10;
    while(true) {
        int error = mdb_txn_begin(self->mdb, nullptr, flags, &txn);
        switch(error) {
        case 0:
            return txn;
        case MDB_MAP_RESIZED:
            if (mapsizePatience > 0) {
                mapsizePatience -= 1;
                error = mdb_env_set_mapsize(self->mdb, 0);
                if(error != 0)
                    throw MdbError(error);
                MDB_envinfo info;
                mdb_env_info(self->mdb, &info);
                continue;
            } else {
                throw MdbError(error);
            }
        default:
            throw MdbError(error);
        }
    }
}

static void txn_commit(MDB_txn* const txn) {
    const int error = mdb_txn_commit(txn);
    if(error != 0)
        throw MdbError(error);
}

static void txn_abort(MDB_txn* const txn) {
    mdb_txn_abort(txn); // This doesn't return any errors.
}

static void open_db(MDB_txn* const txn, const char* const name, unsigned int flags, MDB_dbi* const dbi) {
    const int error = mdb_dbi_open(txn, name, flags | MDB_CREATE, dbi);
    if(error != 0)
        throw MdbError(error);
}

static void put(
    MDB_txn* const txn,
    const MDB_dbi dbi,
    MDB_val* const key,
    MDB_val* const value,
    unsigned int flags = 0
) {
    const int error = mdb_put(txn, dbi, key, value, flags);
    if(error != 0)
        throw MdbError(error);
}

static uint64_t putImmutable(
    MDB_txn* const txn,
    const MDB_dbi dbi,
    MDB_val* const mdbVal,
    const unsigned char typeCode,
    const bool readonly = false
) {
    uint64_t key = SpookyHash::hash64(
        mdbVal->mv_data,
        mdbVal->mv_size,
        typeCode);
    MDB_val mdbKey = { .mv_size = sizeof(key), .mv_data = &key };

    if(readonly) {
        // In a readonly transaction, we turn the put() into a get() to check whether
        // the value is there.
        const int error = mdb_get(txn, dbi, &mdbKey, mdbVal);
        switch(error) {
        case 0:
            // We could check here for hash collisions, but we won't.
            break;
        case MDB_NOTFOUND:
            throw OocError(OocError::ImmutableValueNotFound);
        default:
            throw MdbError(error);
        }
    } else {
        put(txn, dbi, &mdbKey, mdbVal);
    }

    return key;
}

static void del(MDB_txn* const txn, MDB_dbi dbi, MDB_val* key) {
    const int error = mdb_del(txn, dbi, key, nullptr);
    if(error != 0)
        throw MdbError(error);
}

static const uint8_t TYPE_CODE_HARDCODED = 0;
static const uint8_t TYPE_CODE_SHORT_POSITIVE_INT = 1;
static const uint8_t TYPE_CODE_SHORT_NEGATIVE_INT = 2;
static const uint8_t TYPE_CODE_LONG_POSITIVE_INT = 3;
static const uint8_t TYPE_CODE_LONG_NEGATIVE_INT = 4;
static const uint8_t TYPE_CODE_FLOAT = 5;
static const uint8_t TYPE_CODE_UNICODE_WCHAR = 6;
static const uint8_t TYPE_CODE_UNICODE_1BYTE = 7;
static const uint8_t TYPE_CODE_UNICODE_2BYTE = 8;
static const uint8_t TYPE_CODE_UNICODE_4BYTE = 9;
static const uint8_t TYPE_CODE_TUPLE = 10;
static const uint8_t TYPE_CODE_LIST = 11;
static const uint8_t TYPE_CODE_DICT = 12;

// hardcoded values
static const EncodedValue ENCODED_NONE = {.asInt = 0, .typeCode = TYPE_CODE_HARDCODED, .length = 0};
static const EncodedValue ENCODED_INT_ZERO = {.asInt = 1, .typeCode = TYPE_CODE_HARDCODED, .length = 0};
static const EncodedValue ENCODED_TRUE = {.asInt = 3, .typeCode = TYPE_CODE_HARDCODED, .length = 0};
static const EncodedValue ENCODED_FALSE = {.asInt = 4, .typeCode = TYPE_CODE_HARDCODED, .length = 0};
static const EncodedValue ENCODED_EMPTY_TUPLE = {.asInt = 5, .typeCode = TYPE_CODE_HARDCODED, .length = 0};

static void OOCMap_encode(
    OOCMapObject* const self,
    PyObject* const value,
    EncodedValue* const dest,
    MDB_txn* const txn,
    Id2EncodedMap& insertedItemsInThisTransaction,
    const bool readonly = false
) {
    // Python's cell objects
    if(PyCell_Check(value)) {
        OOCMap_encode(self, PyCell_GET(value), dest, txn, insertedItemsInThisTransaction);
        return;
    }

    // did we already write this object?
    const EncodedValue*& destInTheMap = insertedItemsInThisTransaction[value];
    if(destInTheMap != nullptr) {
        *dest = *destInTheMap;
        return;
    }

    // Python's None
    if(value == Py_None) {
        *dest = ENCODED_NONE;
        destInTheMap = dest;
        return;
    }

    // Python's integers
    if(PyLong_CheckExact(value)) {
        PyLongObject* longObject = reinterpret_cast<PyLongObject*>(value);
        if(longObject->ob_base.ob_size == 0) {
            // Integer is 0
            *dest = ENCODED_INT_ZERO;
            destInTheMap = dest;
            return;
        } else {
            const size_t longBufferSize = sizeof(digit) * abs(longObject->ob_base.ob_size);
            if(longBufferSize <= sizeof(dest->asChars)) {
                // Integer fits into EncodedValue directly
                dest->asInt = 0;
                memcpy(dest->asChars, longObject->ob_digit, longBufferSize);
                dest->typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_SHORT_POSITIVE_INT :
                    TYPE_CODE_SHORT_NEGATIVE_INT;
                dest->length = longBufferSize;
                destInTheMap = dest;
                return;
            } else {
                // Integer doesn't fit into EncodedValue, has to be written to the DB
                dest->typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_LONG_POSITIVE_INT :
                    TYPE_CODE_LONG_NEGATIVE_INT;
                dest->length = 0;

                MDB_val mdbValue = { .mv_size = longBufferSize, .mv_data = longObject->ob_digit };

                dest->asUInt = putImmutable(
                    txn,
                    self->intsDb,
                    &mdbValue,
                    dest->typeCode,
                    readonly);
                destInTheMap = dest;
                return;
            }
        }
    }

    // Python's bools
    if(PyBool_Check(value)) {
        if(value == Py_False) {
            *dest = ENCODED_FALSE;
            destInTheMap = dest;
            return;
        } else if(value == Py_True) {
            *dest = ENCODED_TRUE;
            destInTheMap = dest;
            return;
        } else {
            throw OocError(OocError::InvalidBool);
            return;
        }
    }

    // Python's floats
    if(PyFloat_CheckExact(value)) {
        dest->asFloat = PyFloat_AS_DOUBLE(value);
        dest->typeCode = TYPE_CODE_FLOAT;
        dest->length = 0;
        destInTheMap = dest;
        return;
    }

    // Python's complex numbers
    // TODO

    // Python's bytes objects
    // TODO

    // Python's byte array objects
    // TODO

    // Python's unicode objects (strings)
    if(PyUnicode_Check(value)) {
        const int readyError = PyUnicode_READY(value);
        if(readyError != 0)
            throw OocError(OocError::CouldNotReadyString);

        const int kind = PyUnicode_KIND(value);
        size_t dataSize = PyUnicode_GET_LENGTH(value);
        switch(kind) {
        case PyUnicode_WCHAR_KIND:
            dest->typeCode = TYPE_CODE_UNICODE_WCHAR;
            dataSize *= Py_UNICODE_SIZE;
            break;
        case PyUnicode_1BYTE_KIND:
            dest->typeCode = TYPE_CODE_UNICODE_1BYTE;
            dataSize *= sizeof(Py_UCS1);
            break;
        case PyUnicode_2BYTE_KIND:
            dest->typeCode = TYPE_CODE_UNICODE_2BYTE;
            dataSize *= sizeof(Py_UCS2);
            break;
        case PyUnicode_4BYTE_KIND:
            dest->typeCode = TYPE_CODE_UNICODE_4BYTE;
            dataSize *= sizeof(Py_UCS4);
            break;
        default:
            throw OocError(OocError::InvalidStringKind);
        }

        if(dataSize <= sizeof(dest->asChars)) {
            // String fits into one EncodedValue
            dest->length = dataSize;
            memcpy(dest->asChars, PyUnicode_DATA(value), dataSize);
            destInTheMap = dest;
            return;
        } else {
            // String does not fit into one EncodedValue, has to be written to DB
            dest->length = 0;
            MDB_val mdbValue = { .mv_size = dataSize, .mv_data = PyUnicode_DATA(value) };
            dest->asUInt = putImmutable(txn, self->stringsDb, &mdbValue, dest->typeCode, readonly);
            destInTheMap = dest;
            return;
        }
    }

    // Python's tuple objects
    // TODO: this probably doesn't handle namedtuple() correctly
    if(PyTuple_CheckExact(value)) {
        if(PyTuple_GET_SIZE(value) == 0) {
            *dest = ENCODED_EMPTY_TUPLE;
            destInTheMap = dest;
            return;
        } else {
            std::vector<EncodedValue> encodedValues(PyTuple_GET_SIZE(value));
            // TODO: make sure that there are no alignment issues with this array
            for(
                Py_ssize_t i = 0; i < PyTuple_GET_SIZE(value); ++i
            ) {
                OOCMap_encode(
                    self,
                    PyTuple_GET_ITEM(value, i),
                    &encodedValues[i],
                    txn,
                    insertedItemsInThisTransaction,
                    readonly
                );
            }

            dest->length = 0;
            dest->typeCode = TYPE_CODE_TUPLE;
            MDB_val mdbValue = {
                .mv_size = PyTuple_GET_SIZE(value) * sizeof(EncodedValue),
                .mv_data = encodedValues.data()
            };
            dest->asUInt = putImmutable(txn, self->tuplesDb, &mdbValue, dest->typeCode, readonly);

            destInTheMap = dest;
            return;
        }
    }

    // Python's list objects
    if(PyList_CheckExact(value)) {
        dest->typeCode = TYPE_CODE_LIST;
        dest->asListKey.listIndex = std::numeric_limits<uint32_t>::max();
        MDB_val mdbKey = { .mv_size = sizeof(dest->asUInt), .mv_data = &dest->asUInt };

        // find a key
        while(true) {
            dest->asListKey.listId = random_engine();
            MDB_val mdbValue = { .mv_size = sizeof(PyList_GET_SIZE(value)), .mv_data = &PyList_GET_SIZE(value)};
            try {
                put(txn, self->listsDb, &mdbKey, &mdbValue, MDB_NODUPDATA);
            } catch(const MdbError& e) {
                if(e.mdbErrorCode == MDB_KEYEXIST)
                    continue;
                else
                    throw;
            }
            break;
        }

        // We put this into the map now, because the recursive call to _encode() might need it.
        // Lists can contain themselves after all.
        destInTheMap = dest;
        try {
            // add the list elements
            EncodedValue encodedListElement = *dest;
            MDB_val mdbElementKey = {
                .mv_size = sizeof(encodedListElement.asUInt),
                .mv_data = &encodedListElement.asUInt
            };

            for(
                Py_ssize_t i = 0; i < PyList_GET_SIZE(value); ++i
            ) {
                encodedListElement.asListKey.listIndex = i;
                MDB_val mdbElementValue = {.mv_size = sizeof(EncodedValue), .mv_data = nullptr};
                put(txn, self->listsDb, &mdbElementKey, &mdbElementValue, MDB_RESERVE);

                OOCMap_encode(
                    self,
                    PyList_GET_ITEM(value, i),
                    reinterpret_cast<EncodedValue*>(mdbElementValue.mv_data),
                    txn,
                    insertedItemsInThisTransaction,
                    readonly);
            }
        } catch(...) {
            insertedItemsInThisTransaction.erase(value);
            throw;
        }

        return;
    }

    // Python's dict objects
    if(PyDict_CheckExact(value)) {
        // This gets super confusing because we have two key/value stores going at the same
        // time, the PyDict that's stored in *value, and the mdb store. Both of these take
        // keys and values, so the names are all over the place.

        uint32_t dictId;
        MDB_val mdbKey = { .mv_size = sizeof(dictId), .mv_data = &dictId };

        Py_ssize_t dictSize = PyDict_Size(value);
        MDB_val mdbValue = { .mv_size = sizeof(dictSize), .mv_data = &dictSize };

        // find a key
        while(true) {
            dictId = random_engine();
            try {
                put(txn, self->dictsDb, &mdbKey, &mdbValue, MDB_NODUPDATA);
            } catch(const MdbError& e) {
                if(e.mdbErrorCode == MDB_KEYEXIST)
                    continue;
                else
                    throw;
            }
            break;
        }

        // We put this into the map now, because the recursive call to _encode() might need it.
        // Dicts can contain themselves after all.
        dest->asDictKey.dictId = dictId;
        dest->asDictKey.reserved = 0;
        dest->typeCode = TYPE_CODE_DICT;
        destInTheMap = dest;
        try {
            // insert the items
            PyObject* pyKey;
            PyObject* pyValue;
            Py_ssize_t pos = 0;
            DictItemKey dictItemKey = { .dictId = dictId };
            while(PyDict_Next(value, &pos, &pyKey, &pyValue)) {
                // write the PyDict key, filling in the value we need for the mdb key
                OOCMap_encode(
                    self,
                    pyKey,
                    &dictItemKey.key,
                    txn,
                    insertedItemsInThisTransaction,
                    readonly);

                // write the PyDict value
                EncodedValue encodedValue;
                OOCMap_encode(
                    self,
                    pyValue,
                    &encodedValue,
                    txn,
                    insertedItemsInThisTransaction,
                    readonly);

                // Write the mdb key/value.
                MDB_val mdbDictItemKey = {.mv_size = sizeof(dictItemKey), .mv_data = &dictItemKey};
                MDB_val mdbDictItemValue = {.mv_size = sizeof(encodedValue), .mv_data = &encodedValue};
                put(txn, self->dictsDb, &mdbDictItemKey, &mdbDictItemValue);
            }
        } catch(...) {
            insertedItemsInThisTransaction.erase(value);
            throw;
        }

        return;
    }

    // Python's set objects
    // TODO

    throw UnknownTypeError(PyObject_Type(value));
}

static bool isOOCMap(PyObject* self);

//
// Methods that are directly exposed to Python
// These are not allowed to throw exceptions.
//

static void OOCMap_dealloc(OOCMapObject* self) {
    mdb_env_close(self->mdb);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* OOCMap_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    OOCMapObject* self = (OOCMapObject*)type->tp_alloc(type, 0);
    if(self == nullptr) {
        PyErr_NoMemory();
    } else {
        mdb_env_create(&self->mdb);
        mdb_env_set_maxdbs(self->mdb, 6);
    }
    return (PyObject*)self;
}

static int OOCMap_init(OOCMapObject* self, PyObject* args, PyObject* kwds) {
    // parse parameters
    static const char *kwlist[] = {"filename", "mapsize", nullptr};
    PyObject* filenameObject = nullptr;
    unsigned long long mapsize = 0;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "O&|$K",
            const_cast<char**>(kwlist),
            PyUnicode_FSConverter, &filenameObject, &mapsize);
    if(!parseSuccess)
        return -1;
    const char* filename = PyBytes_AS_STRING(filenameObject);

    // set mapsize
    if(mapsize == 0) mapsize = 1024ull * 1024ull * 1024ull * 1024ull;
    const int setMapsizeError = mdb_env_set_mapsize(self->mdb, mapsize);
    if(setMapsizeError != 0) {
        Py_XDECREF(filenameObject);
        MdbError(setMapsizeError).pythonize();
        return -1;
    }

    // open lmdb
    // These are some aggressive flags that don't guarantee data integrity.
    const int mdbOpenError = mdb_env_open(
            self->mdb,
            filename,
            MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC| MDB_MAPASYNC | MDB_NOMEMINIT,
            0644);
    Py_CLEAR(filenameObject);
    if(mdbOpenError != 0) {
        MdbError(mdbOpenError).pythonize();
        return -1;
    }
    MDB_envinfo info;
    mdb_env_info(self->mdb, &info);
    // TODO: We should check for and handle the case where self->mdb has already been opened.

    // open all the DBs
    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self, true);
        open_db(txn, "root", MDB_CREATE, &self->rootDb);
        open_db(txn, "ints", MDB_CREATE | MDB_INTEGERKEY, &self->intsDb);
        open_db(txn, "strings", MDB_CREATE | MDB_INTEGERKEY, &self->stringsDb);
        open_db(txn, "lists", MDB_CREATE | MDB_INTEGERKEY, &self->listsDb);
        open_db(txn, "tuples", MDB_CREATE | MDB_INTEGERKEY, &self->tuplesDb);
        open_db(txn, "dicts", MDB_CREATE, &self->dictsDb);
        txn_commit(txn);
    } catch (const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }

    return 0;
}

static Py_ssize_t OOCMap_length(PyObject* pySelf) {
    if(!isOOCMap(pySelf)) {
        PyErr_BadArgument();
        return -1;
    }
    OOCMapObject* self = reinterpret_cast<OOCMapObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self, false);
        MDB_stat stat;
        mdb_stat(txn, self->rootDb, &stat);
        txn_commit(txn);
        return stat.ms_entries;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }
}

static int OOCMap_insert(PyObject* pySelf, PyObject* key, PyObject* value) {
    // cast the input
    if(!isOOCMap(pySelf)) {
        PyErr_BadArgument();
        return -1;
    }
    OOCMapObject* self = reinterpret_cast<OOCMapObject*>(pySelf);

    // start transaction
    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self, true);
        Id2EncodedMap insertedItemsInThisTransaction;

        EncodedValue encodedKey;
        OOCMap_encode(self, key, &encodedKey, txn, insertedItemsInThisTransaction);
        MDB_val mdbKey = { .mv_size=sizeof(encodedKey), .mv_data=&encodedKey };

        if(value == nullptr) {
            // Deleting the value
            del(txn, self->rootDb, &mdbKey);
        } else {
            // Inserting a new value
            EncodedValue encodedValue;
            OOCMap_encode(self, value, &encodedValue, txn, insertedItemsInThisTransaction);
            MDB_val mdbValue = { .mv_size=sizeof(encodedValue), .mv_data=&encodedValue };

            put(txn, self->rootDb, &mdbKey, &mdbValue);
        }
        txn_commit(txn);
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }

    return 0;
}



//
// Python definitions to tie it all together
//

static PyMethodDef OOCMap_methods[] = {
// We're not using these anymore, but I'm leaving the code here as an example.
//        {
//            "begin_transaction",
//            (PyCFunction)OOCMap_begin_transaction,
//            METH_NOARGS,
//            PyDoc_STR("start a transaction")
//        },{
//            "end_transaction",
//            (PyCFunction)OOCMap_end_transaction,
//            METH_NOARGS,
//            PyDoc_STR("end a transaction")
//        },
        {nullptr}, // sentinel
};

static PyMappingMethods OOCMap_mapping_methods = {
        .mp_length = OOCMap_length,
        .mp_subscript = nullptr,
        .mp_ass_subscript = OOCMap_insert
};

static PyTypeObject OOCMapType = {
        PyVarObject_HEAD_INIT(nullptr, 0)
        .tp_name = "oocmap.OOCMap",
        .tp_basicsize = sizeof(OOCMapObject),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)OOCMap_dealloc,
        .tp_as_mapping = &OOCMap_mapping_methods,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "The out-of-core map",
        .tp_methods = OOCMap_methods,
        .tp_init = (initproc)OOCMap_init,
        .tp_new = OOCMap_new,
};

static inline bool isOOCMap(PyObject* const self) {
    return self->ob_type == &OOCMapType;
}

static PyMethodDef OocmapMethods[] = {
    {nullptr, nullptr, 0, nullptr}        /* Sentinel */
};

static struct PyModuleDef oocmap_module = {
    PyModuleDef_HEAD_INIT,
    "oocmap",   /* name of module */
    "A Python dictionary that reads and writes its contents to disk.",
    -1,
    OocmapMethods
};

PyMODINIT_FUNC PyInit_oocmap() {
    if(PyType_Ready(&OOCMapType) < 0)
        return nullptr;

    PyObject* const m = PyModule_Create(&oocmap_module);
    if(m == nullptr)
        return nullptr;

    Py_INCREF(&OOCMapType);
    if(PyModule_AddObject(m, "OOCMap", (PyObject*)&OOCMapType) < 0) {
        Py_DECREF(&OOCMapType);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}
