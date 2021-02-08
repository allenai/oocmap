#include "oocmap.h"

#include <memory>
#include <random>
#include "spooky.h"

#include "errors.h"
#include "db.h"
#include "lazytuple.h"
#include "lazylist.h"

static std::mt19937 random_engine(std::chrono::system_clock::now().time_since_epoch().count());

// This is the structure that defines the keys in the dicts table. Dicts are different
// because the keys are not integers but variable-length.
struct DictItemKey {
    uint32_t dictId;
    EncodedValue key;
};


//
// Functions that are not exposed to Python
// These are allowed to throw exceptions.
//

// hardcoded values
static const EncodedValue ENCODED_NONE = {.asInt = 0, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_INT_ZERO = {.asInt = 1, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_TRUE = {.asInt = 3, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_FALSE = {.asInt = 4, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_EMPTY_TUPLE = {.asInt = 5, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_EMPTY_STRING = {.asInt = 6, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};

void OOCMap_encode(
    OOCMapObject* const self,
    PyObject* const value,
    EncodedValue* const dest,
    MDB_txn* const txn,
    Id2EncodedMap& insertedItemsInThisTransaction,
    const bool readonly
) {
    // Python's cell objects
    if(PyCell_Check(value)) {
        OOCMap_encode(self, PyCell_GET(value), dest, txn, insertedItemsInThisTransaction, readonly);
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

    // TODO: Do this in terms of "protocols", not concrete objects (https://docs.python.org/3/c-api/mapping.html and friends)

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
                dest->asUInt = 0;
                memcpy(dest->asChars, longObject->ob_digit, longBufferSize);
                dest->typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_SHORT_POSITIVE_INT :
                    TYPE_CODE_SHORT_NEGATIVE_INT;
                dest->lengthMinusOne = longBufferSize - 1;
                destInTheMap = dest;
                return;
            } else {
                // Integer doesn't fit into EncodedValue, has to be written to the DB
                dest->typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_LONG_POSITIVE_INT :
                    TYPE_CODE_LONG_NEGATIVE_INT;
                dest->lengthMinusOne = 0;

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
        dest->lengthMinusOne = 0;
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
        size_t dataSize = PyUnicode_GET_LENGTH(value);
        if(dataSize == 0) {
            *dest = ENCODED_EMPTY_STRING;
            destInTheMap = dest;
            return;
        } else {
            const int kind = PyUnicode_KIND(value);
            switch(kind) {
            case PyUnicode_WCHAR_KIND:
                dest->typeCode = TYPE_CODE_UNICODE_SHORT_WCHAR;
                dataSize *= Py_UNICODE_SIZE;
                break;
            case PyUnicode_1BYTE_KIND:
                dest->typeCode = TYPE_CODE_UNICODE_SHORT_1BYTE;
                dataSize *= sizeof(Py_UCS1);
                break;
            case PyUnicode_2BYTE_KIND:
                dest->typeCode = TYPE_CODE_UNICODE_SHORT_2BYTE;
                dataSize *= sizeof(Py_UCS2);
                break;
            case PyUnicode_4BYTE_KIND:
                dest->typeCode = TYPE_CODE_UNICODE_SHORT_4BYTE;
                dataSize *= sizeof(Py_UCS4);
                break;
            default:
                throw OocError(OocError::InvalidStringKind);
            }

            if(dataSize <= sizeof(dest->asChars)) {
                // String fits into one EncodedValue
                dest->lengthMinusOne = dataSize - 1;
                dest->asUInt = 0;
                memcpy(dest->asChars, PyUnicode_DATA(value), dataSize);
                destInTheMap = dest;
                return;
            } else {
                // String does not fit into one EncodedValue, has to be written to DB
                dest->lengthMinusOne = 0;
                dest->typeCode += TYPE_CODE_UNICODE_LONG_SHORT_OFFSET;
                MDB_val mdbValue = {.mv_size = dataSize, .mv_data = PyUnicode_DATA(value)};
                dest->asUInt = putImmutable(txn, self->stringsDb, &mdbValue, dest->typeCode, readonly);
                destInTheMap = dest;
                return;
            }
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

            dest->lengthMinusOne = 0;
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

            for(Py_ssize_t i = 0; i < PyList_GET_SIZE(value); ++i) {
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

    // LazyTuple objects
    if(value->ob_type == &OOCLazyTupleType) {
        OOCLazyTupleObject* const tupleValue = reinterpret_cast<OOCLazyTupleObject*>(value);
        if(tupleValue->ooc == self) {
            dest->asUInt = tupleValue->tupleId;
            dest->typeCode = TYPE_CODE_TUPLE;
            destInTheMap = dest;
            return;
        } else {
            PyObject* const eager = OOCLazyTupleObject_eager(tupleValue, txn);
            OOCMap_encode(self, eager, dest, txn, insertedItemsInThisTransaction, readonly);
            destInTheMap = dest;
            Py_DECREF(eager);
        }
    }

    throw UnknownTypeError(PyObject_Type(value));
}

PyObject* OOCMap_decode(
    OOCMapObject* const self,
    EncodedValue* const encodedValue,
    MDB_txn* const txn
    // We don't need a cache of objects we have decoded. Because of lazyness, we only ever decode
    // one object at a time.
) {
    switch(encodedValue->typeCode) {
    case TYPE_CODE_HARDCODED: {
        PyObject* result = nullptr;
        switch(encodedValue->asInt) {
        case 0:
            Py_INCREF(Py_None);
            return Py_None;
        case 1:
            result = PyLong_FromLong(0);
            break;
        case 3:
            Py_INCREF(Py_True);
            return Py_True;
        case 4:
            Py_INCREF(Py_False);
            return Py_False;
        case 5:
            result = PyTuple_New(0);
            break;
        case 6:
            result = PyUnicode_New(0, 127);
            break;
        default:
            throw OocError(OocError::UnknownHardcodedValue);
        }
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        return result;
    }
    case TYPE_CODE_SHORT_POSITIVE_INT:
    case TYPE_CODE_SHORT_NEGATIVE_INT: {
        const size_t length = encodedValue->lengthMinusOne + 1;
        PyLongObject* const result = _PyLong_New(length / sizeof(digit));
        // TODO: Every duplicate long will create its own PyObject this way. We should cache
        // them and return the same ones multiple times if possible.
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        if(encodedValue->typeCode == TYPE_CODE_SHORT_NEGATIVE_INT)
            result->ob_base.ob_size *= -1;
        memcpy(result->ob_digit, encodedValue->asChars, length);
        return (PyObject*)result;
    }
    case TYPE_CODE_LONG_POSITIVE_INT:
    case TYPE_CODE_LONG_NEGATIVE_INT: {
        MDB_val mdbKey = { .mv_size = sizeof(encodedValue->asUInt), .mv_data = &(encodedValue->asUInt) };
        MDB_val mdbValue;
        get(txn, self->intsDb, &mdbKey, &mdbValue);

        PyLongObject* const result = _PyLong_New(mdbValue.mv_size / sizeof(digit));
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        if(encodedValue->typeCode == TYPE_CODE_LONG_NEGATIVE_INT)
            result->ob_base.ob_size *= -1;
        memcpy(result->ob_digit, mdbValue.mv_data, mdbValue.mv_size);
        return (PyObject*)result;
    }
    case TYPE_CODE_FLOAT: {
        PyObject* const result = PyFloat_FromDouble(encodedValue->asFloat);
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        return result;
    }
    case TYPE_CODE_UNICODE_SHORT_WCHAR:
    case TYPE_CODE_UNICODE_SHORT_1BYTE:
    case TYPE_CODE_UNICODE_SHORT_2BYTE:
    case TYPE_CODE_UNICODE_SHORT_4BYTE: {
        Py_ssize_t size = encodedValue->lengthMinusOne + 1;
        int kind;
        switch(encodedValue->typeCode) {
        case TYPE_CODE_UNICODE_SHORT_WCHAR:
            size /= Py_UNICODE_SIZE;
            kind = PyUnicode_WCHAR_KIND;
            break;
        case TYPE_CODE_UNICODE_SHORT_1BYTE:
            size /= sizeof(Py_UCS1);
            kind = PyUnicode_1BYTE_KIND;
            break;
        case TYPE_CODE_UNICODE_SHORT_2BYTE:
            size /= sizeof(Py_UCS2);
            kind = PyUnicode_2BYTE_KIND;
            break;
        case TYPE_CODE_UNICODE_SHORT_4BYTE:
            size /= sizeof(Py_UCS4);
            kind = PyUnicode_4BYTE_KIND;
            break;
        default:
            throw OocError(OocError::UnexpectedData);
        }
        PyObject* const result = PyUnicode_FromKindAndData(kind, encodedValue->asChars, size);
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        return result;
    }
    case TYPE_CODE_UNICODE_LONG_WCHAR:
    case TYPE_CODE_UNICODE_LONG_1BYTE:
    case TYPE_CODE_UNICODE_LONG_2BYTE:
    case TYPE_CODE_UNICODE_LONG_4BYTE: {
        MDB_val mdbKey = {.mv_size = sizeof(encodedValue->asUInt), .mv_data = &(encodedValue->asUInt)};
        MDB_val mdbValue;
        get(txn, self->stringsDb, &mdbKey, &mdbValue);

        Py_ssize_t size = mdbValue.mv_size;
        int kind;
        switch(encodedValue->typeCode) {
        case TYPE_CODE_UNICODE_LONG_WCHAR:
            size /= Py_UNICODE_SIZE;
            kind = PyUnicode_WCHAR_KIND;
            break;
        case TYPE_CODE_UNICODE_LONG_1BYTE:
            size /= sizeof(Py_UCS1);
            kind = PyUnicode_1BYTE_KIND;
            break;
        case TYPE_CODE_UNICODE_LONG_2BYTE:
            size /= sizeof(Py_UCS2);
            kind = PyUnicode_2BYTE_KIND;
            break;
        case TYPE_CODE_UNICODE_LONG_4BYTE:
            size /= sizeof(Py_UCS4);
            kind = PyUnicode_4BYTE_KIND;
            break;
        default:
            throw OocError(OocError::UnexpectedData);
        }
        PyObject* const result = PyUnicode_FromKindAndData(kind, mdbValue.mv_data, size);
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        return result;
    }
    case TYPE_CODE_TUPLE:
        return reinterpret_cast<PyObject*>(OOCLazyTuple_fastnew(self, encodedValue->asUInt));
    case TYPE_CODE_LIST:
        return reinterpret_cast<PyObject*>(OOCLazyList_fastnew(self, encodedValue->asUInt));
    case TYPE_CODE_DICT:
        // TODO
    default:
        throw OocError(OocError::UnknownType);
    }
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
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCMapObject* self = reinterpret_cast<OOCMapObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
    } else {
        const int error = mdb_env_create(&self->mdb);
        if(error != 0) {
            type->tp_dealloc(pySelf);
            MdbError(error).pythonize();
            return nullptr;
        }
        mdb_env_set_maxdbs(self->mdb, 6);
    }
    return (PyObject*)self;
}

static int OOCMap_init(OOCMapObject* self, PyObject* args, PyObject* kwds) {
    // parse parameters
    static const char *kwlist[] = {"filename", "max_size", nullptr};
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
        txn = txn_begin(self->mdb, true);
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
    OOCMapObject* const self = reinterpret_cast<OOCMapObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->mdb, false);
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
        txn = txn_begin(self->mdb, true);
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

static PyObject* OOCMap_get(PyObject* pySelf, PyObject* key) {
    // cast the input
    if(!isOOCMap(pySelf)) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCMapObject* self = reinterpret_cast<OOCMapObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->mdb, false);
        Id2EncodedMap insertedItemsInThisTransaction;

        EncodedValue encodedKey;
        OOCMap_encode(self, key, &encodedKey, txn, insertedItemsInThisTransaction, true);
        MDB_val mdbKey = {.mv_size=sizeof(encodedKey), .mv_data=&encodedKey};

        MDB_val mdbValue;
        get(txn, self->rootDb, &mdbKey, &mdbValue);
        if(mdbValue.mv_size != sizeof(EncodedValue))
            throw OocError(OocError::UnexpectedData);
        EncodedValue* encodedValue = static_cast<EncodedValue*>(mdbValue.mv_data);

        PyObject* const result = OOCMap_decode(self, encodedValue, txn);
        txn_commit(txn);
        return result;
    } catch(const MdbError& error) {
        if(txn != nullptr) txn_abort(txn);
        if(error.mdbErrorCode == MDB_NOTFOUND)
            PyErr_SetObject(PyExc_KeyError, key);
        else
            error.pythonize();
        return nullptr;
    } catch(const OocError& error) {
        if(txn != nullptr) txn_abort(txn);
        if(error.errorCode == OocError::ImmutableValueNotFound)
            PyErr_SetObject(PyExc_KeyError, key);
        else
            error.pythonize();
        return nullptr;
    }
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
        .mp_subscript = OOCMap_get,
        .mp_ass_subscript = OOCMap_insert
};

PyTypeObject OOCMapType = {
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