#include "oocmap.h"

#include <memory>
#include <random>
#include "spooky.h"

#include "errors.h"
#include "db.h"
#include "lazytuple.h"
#include "lazylist.h"
#include "lazydict.h"

static std::mt19937 random_engine(std::chrono::system_clock::now().time_since_epoch().count());

const uint32_t ListKey::listIndexLength = std::numeric_limits<uint32_t>::max();

OOCTransaction::OOCTransaction(OOCMapObject* const ooc, const bool readonly) :
    readonly(readonly),
    txnOwned(true),
    txn(txn_begin(ooc->mdb, !readonly))
{ }

OOCTransaction::OOCTransaction(MDB_txn* const txn, const bool readonly) :
    readonly(readonly),
    txnOwned(false),
    txn(txn)
{ }


OOCTransaction::~OOCTransaction() {
    if(txnOwned && txn != nullptr)
        abort();
    else
        clear();
}

void OOCTransaction::clear() {
    for(const auto& pair : insertedItems) Py_DECREF(pair.first);
    insertedItems.clear();
}

void OOCTransaction::commit() {
    txn_commit(txn);
    txn = nullptr;
    clear();
}

void OOCTransaction::abort() {
    txn_abort(txn);
    txn = nullptr;
    clear();
}


//
// Functions that are not exposed to Python
// These are allowed to throw exceptions.
//

// hardcoded values
static const EncodedValue ENCODED_UNINITIALIZED = {.asUInt = 0, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0}; // This one has to be all zeros.
static const EncodedValue ENCODED_NONE = {.asUInt = 1, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_INT_ZERO = {.asUInt = 2, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_TRUE = {.asUInt = 3, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_FALSE = {.asUInt = 4, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_EMPTY_TUPLE = {.asUInt = 5, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};
static const EncodedValue ENCODED_EMPTY_STRING = {.asUInt = 6, .typeCode = TYPE_CODE_HARDCODED, .lengthMinusOne = 0};

const EncodedValue* OOCMap_encode(
    OOCMapObject* const self,
    PyObject* const value,
    OOCTransaction& txn,
    const bool failOnMutable,
    const bool failOnWrite
) {
    // Python's cell objects
    if(PyCell_Check(value)) {
        return OOCMap_encode(self, PyCell_GET(value), txn, failOnMutable, failOnWrite);
    }

    // Python's None
    if(value == Py_None) {
        return &ENCODED_NONE;
    }

    // did we already write this object?
    EncodedValue& result = txn.insertedItems[value];
    Py_INCREF(value);   // insertedItems owns these objects
    if(result != ENCODED_UNINITIALIZED) return &result;

    // Python's integers
    if(PyLong_CheckExact(value)) {
        PyLongObject* longObject = reinterpret_cast<PyLongObject*>(value);
        if(longObject->ob_base.ob_size == 0) {
            // Integer is 0
            result = ENCODED_INT_ZERO;
            return &result;
        } else {
            const size_t longBufferSize = sizeof(digit) * abs(longObject->ob_base.ob_size);
            if(longBufferSize <= sizeof(result.asChars)) {
                // Integer fits into EncodedValue directly
                result.asUInt = 0;
                memcpy(result.asChars, longObject->ob_digit, longBufferSize);
                result.typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_SHORT_POSITIVE_INT :
                    TYPE_CODE_SHORT_NEGATIVE_INT;
                result.lengthMinusOne = longBufferSize - 1;
                return &result;
            } else {
                // Integer doesn't fit into EncodedValue, has to be written to the DB
                result.typeCode =
                    longObject->ob_base.ob_size > 0 ?
                    TYPE_CODE_LONG_POSITIVE_INT :
                    TYPE_CODE_LONG_NEGATIVE_INT;
                result.lengthMinusOne = 0;

                MDB_val mdbValue = { .mv_size = longBufferSize, .mv_data = longObject->ob_digit };

                result.asUInt = putImmutable(
                    txn.txn,
                    self->intsDb,
                    &mdbValue,
                    result.typeCode,
                    txn.readonly || failOnWrite);
                return &result;
            }
        }
    }

    // Python's bools
    if(PyBool_Check(value)) {
        if(value == Py_False) {
            result = ENCODED_FALSE;
            return &result;
        } else if(value == Py_True) {
            result = ENCODED_TRUE;
            return &result;
        } else {
            throw OocError(OocError::InvalidBool);
        }
    }

    // Python's floats
    if(PyFloat_CheckExact(value)) {
        result.asFloat = PyFloat_AS_DOUBLE(value);
        result.typeCode = TYPE_CODE_FLOAT;
        result.lengthMinusOne = 0;
        return &result;
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
            result = ENCODED_EMPTY_STRING;
            return &result;
        } else {
            const int kind = PyUnicode_KIND(value);
            switch(kind) {
            case PyUnicode_WCHAR_KIND:
                result.typeCode = TYPE_CODE_UNICODE_SHORT_WCHAR;
                dataSize *= Py_UNICODE_SIZE;
                break;
            case PyUnicode_1BYTE_KIND:
                result.typeCode = TYPE_CODE_UNICODE_SHORT_1BYTE;
                dataSize *= sizeof(Py_UCS1);
                break;
            case PyUnicode_2BYTE_KIND:
                result.typeCode = TYPE_CODE_UNICODE_SHORT_2BYTE;
                dataSize *= sizeof(Py_UCS2);
                break;
            case PyUnicode_4BYTE_KIND:
                result.typeCode = TYPE_CODE_UNICODE_SHORT_4BYTE;
                dataSize *= sizeof(Py_UCS4);
                break;
            default:
                throw OocError(OocError::InvalidStringKind);
            }

            if(dataSize <= sizeof(result.asChars)) {
                // String fits into one EncodedValue
                result.lengthMinusOne = dataSize - 1;
                result.asUInt = 0;
                memcpy(result.asChars, PyUnicode_DATA(value), dataSize);
                return &result;
            } else {
                // String does not fit into one EncodedValue, has to be written to DB
                result.lengthMinusOne = 0;
                result.typeCode += TYPE_CODE_UNICODE_LONG_SHORT_OFFSET;
                MDB_val mdbValue = {.mv_size = dataSize, .mv_data = PyUnicode_DATA(value)};
                result.asUInt = putImmutable(
                    txn.txn,
                    self->stringsDb,
                    &mdbValue,
                    result.typeCode,
                    txn.readonly || failOnWrite);
                return &result;
            }
        }
    }

    // Python's tuple objects
    // TODO: this probably doesn't handle namedtuple() correctly
    if(PyTuple_CheckExact(value)) {
        if(PyTuple_GET_SIZE(value) == 0) {
            result = ENCODED_EMPTY_TUPLE;
            return &result;
        } else {
            std::vector<EncodedValue> encodedValues(PyTuple_GET_SIZE(value));
            for(
                Py_ssize_t i = 0; i < PyTuple_GET_SIZE(value); ++i
            ) {
                encodedValues[i] = *OOCMap_encode(self,PyTuple_GET_ITEM(value, i),txn, failOnMutable, failOnWrite);
            }

            result.lengthMinusOne = 0;
            result.typeCode = TYPE_CODE_TUPLE;
            MDB_val mdbValue = {
                .mv_size = PyTuple_GET_SIZE(value) * sizeof(EncodedValue),
                .mv_data = encodedValues.data()
            };
            result.asUInt = putImmutable(
                txn.txn,
                self->tuplesDb,
                &mdbValue,
                result.typeCode,
                txn.readonly || failOnWrite
            );
            return &result;
        }
    }

    // Python's list objects
    if(PyList_CheckExact(value)) {
        if(failOnMutable)
            throw OocError(OocError::MutableValueNotAllowed);
        if(failOnWrite)
            throw OocError(OocError::WriteNotAllowed);

        result.typeCode = TYPE_CODE_LIST;
        result.asListKey.listIndex = ListKey::listIndexLength;
        MDB_val mdbKey = { .mv_size = sizeof(result.asListKey), .mv_data = &result.asListKey };
        uint32_t length = Py_SIZE(value);

        // find a key
        while(true) {
            result.asListKey.listId = random_engine();
            MDB_val mdbValue = { .mv_size = sizeof(uint32_t), .mv_data = &length};
            try {
                put(txn.txn, self->listsDb, &mdbKey, &mdbValue, MDB_NODUPDATA);
            } catch(const MdbError& e) {
                if(e.mdbErrorCode == MDB_KEYEXIST) {
                    continue;
                } else {
                    result = ENCODED_UNINITIALIZED;
                    throw;
                }
            }
            break;
        }

        try {
            // add the list elements
            EncodedValue encodedListElement = result;
            MDB_val mdbElementKey = {
                .mv_size = sizeof(encodedListElement.asListKey),
                .mv_data = &encodedListElement.asListKey
            };

            for(Py_ssize_t i = 0; i < PyList_GET_SIZE(value); ++i) {
                const EncodedValue* const encodedItem = OOCMap_encode(self, PyList_GET_ITEM(value, i), txn, failOnMutable, failOnWrite);

                encodedListElement.asListKey.listIndex = i;
                MDB_val mdbElementValue = {
                    .mv_size = sizeof(EncodedValue),
                    .mv_data = const_cast<EncodedValue*>(encodedItem)
                };
                put(txn.txn, self->listsDb, &mdbElementKey, &mdbElementValue);
            }
        } catch(...) {
            // We already filled in `result` above, so we need to explicitly clear it now.
            result = ENCODED_UNINITIALIZED;
            throw;
        }

        return &result;
    }

    // Python's dict objects
    if(PyDict_CheckExact(value)) {
        if(failOnMutable)
            throw OocError(OocError::MutableValueNotAllowed);
        if(failOnWrite)
            throw OocError(OocError::WriteNotAllowed);

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
                put(txn.txn, self->dictsDb, &mdbKey, &mdbValue, MDB_NODUPDATA);
            } catch(const MdbError& e) {
                if(e.mdbErrorCode == MDB_KEYEXIST) {
                    continue;
                } else {
                    result = ENCODED_UNINITIALIZED;
                    throw;
                }
            }
            break;
        }

        // We put this into the map now, because the recursive call to _encode() might need it.
        // Dicts can contain themselves after all.
        result.asDictKey.dictId = dictId;
        result.asDictKey.reserved = 0;
        result.typeCode = TYPE_CODE_DICT;
        try {
            // insert the items
            PyObject* pyKey;
            PyObject* pyValue;
            Py_ssize_t pos = 0;
            DictItemKey dictItemKey = { .dictId = dictId };
            while(PyDict_Next(value, &pos, &pyKey, &pyValue)) {
                // write the PyDict key, filling in the value we need for the mdb key
                dictItemKey.key = *OOCMap_encode(self, pyKey,txn, true, failOnWrite);

                // write the PyDict value
                const EncodedValue* const encodedValue = OOCMap_encode(self, pyValue, txn, failOnMutable, failOnWrite);

                // Write the mdb key/value.
                MDB_val mdbDictItemKey = {.mv_size = sizeof(dictItemKey), .mv_data = &dictItemKey};
                MDB_val mdbDictItemValue = {
                    .mv_size = sizeof(*encodedValue),
                    .mv_data = const_cast<EncodedValue*>(encodedValue)
                };
                put(txn.txn, self->dictsDb, &mdbDictItemKey, &mdbDictItemValue);
            }
        } catch(...) {
            // We already filled in `result` above, so we need to clear it now.
            result = ENCODED_UNINITIALIZED;
            throw;
        }

        return &result;
    }

    // Python's set objects
    // TODO

    // LazyTuple objects
    if(value->ob_type == &OOCLazyTupleType) {
        OOCLazyTupleObject* const tupleValue = reinterpret_cast<OOCLazyTupleObject*>(value);
        if(tupleValue->ooc == self) {
            result.asUInt = tupleValue->tupleId;
            result.typeCode = TYPE_CODE_TUPLE;
            result.lengthMinusOne = 0;
            return &result;
        } else {
            if(failOnWrite)
                throw OocError(OocError::WriteNotAllowed);

            OOCTransaction otherTxn(tupleValue->ooc, true);
            PyObject* const eager = OOCLazyTupleObject_eager(tupleValue, otherTxn);
            try {
                txn.commit();
                const EncodedValue* const encoded = OOCMap_encode(self, eager, txn, failOnMutable, false);
                Py_DECREF(eager);
                result = *encoded;
            } catch(...) {
                Py_DECREF(eager);
                throw;
            }
            return &result;
        }
    }

    // LazyList objects
    if(value->ob_type == &OOCLazyListType) {
        if(failOnMutable)
            throw OocError(OocError::MutableValueNotAllowed);

        OOCLazyListObject* const listValue = reinterpret_cast<OOCLazyListObject*>(value);
        if(listValue->ooc == self) {
            result.asListKey.listId = listValue->listId;
            result.asListKey.listIndex = std::numeric_limits<uint32_t>::max();
            result.typeCode = TYPE_CODE_LIST;
            result.lengthMinusOne = 0;
            return &result;
        } else {
            if(failOnWrite)
                throw OocError(OocError::WriteNotAllowed);

            OOCTransaction otherTxn(listValue->ooc, true);
            PyObject* const eager = OOCLazyListObject_eager(listValue, otherTxn);
            try {
                otherTxn.commit();
                const EncodedValue* const encoded = OOCMap_encode(self, eager, txn, failOnMutable, false);
                Py_DECREF(eager);
                result = *encoded;
            } catch(...) {
                Py_DECREF(eager);
                throw;
            }

            return &result;
        }
    }

    // LazyDict objects
    if(value->ob_type == &OOCLazyDictType) {
        if(failOnMutable)
            throw OocError(OocError::MutableValueNotAllowed);

        OOCLazyDictObject* const dictValue = reinterpret_cast<OOCLazyDictObject*>(value);
        if(dictValue->ooc == self) {
            result.asDictKey.dictId = dictValue->dictId;
            result.asDictKey.reserved = 0;
            result.typeCode = TYPE_CODE_DICT;
            result.lengthMinusOne = 0;
            return &result;
        } else {
            if(failOnWrite)
                throw OocError(OocError::WriteNotAllowed);

            OOCTransaction otherTxn(dictValue->ooc, true);
            PyObject* const eager = OOCLazyDictObject_eager(dictValue, otherTxn);
            try {
                otherTxn.commit();
                const EncodedValue* const encoded = OOCMap_encode(self, eager, txn, failOnMutable, false);
                Py_DECREF(eager);
                result = *encoded;
            } catch(...) {
                Py_DECREF(eager);
                throw;
            }
            return &result;
        }
    }

    throw UnknownTypeError(PyObject_Type(value));
}

PyObject* OOCMap_decode(
    OOCMapObject* const self,
    EncodedValue* const encodedValue,
    OOCTransaction& txn
    // We don't need a cache of objects we have decoded. Because of lazyness, we only ever decode
    // one object at a time.
) {
    switch(encodedValue->typeCode) {
    case TYPE_CODE_HARDCODED: {
        PyObject* result = nullptr;
        switch(encodedValue->asInt) {
        case 1:
            Py_RETURN_NONE;
        case 2:
            result = PyLong_FromLong(0);
            break;
        case 3:
            Py_RETURN_TRUE;
        case 4:
            Py_RETURN_FALSE;
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
        const bool found = get(txn.txn, self->intsDb, &mdbKey, &mdbValue);
        if(!found) throw OocError(OocError::UnexpectedData);

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
        const bool found = get(txn.txn, self->stringsDb, &mdbKey, &mdbValue);
        if(!found) throw OocError(OocError::UnexpectedData);

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
        return reinterpret_cast<PyObject*>(OOCLazyList_fastnew(self, encodedValue->asListKey.listId));
    case TYPE_CODE_DICT:
        return reinterpret_cast<PyObject*>(OOCLazyDict_fastnew(self, encodedValue->asDictKey.dictId));
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
    if(mapsize == 0) mapsize = 1024ull * 1024ull * 1024ull;
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
            MDB_NOSUBDIR | MDB_NOSYNC | MDB_WRITEMAP | MDB_NOMETASYNC| MDB_MAPASYNC | MDB_NOMEMINIT | MDB_NOTLS,
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
    try {
        OOCTransaction txn(self, false);

        const EncodedValue* const encodedKey = OOCMap_encode(self, key, txn, true);
        MDB_val mdbKey = {
            .mv_size = sizeof(*encodedKey),
            .mv_data = const_cast<EncodedValue*>(encodedKey)
        };

        if(value == nullptr) {
            // Deleting the value
            del(txn.txn, self->rootDb, &mdbKey);
        } else {
            // Inserting a new value
            const EncodedValue* const encodedValue = OOCMap_encode(self, value, txn);
            MDB_val mdbValue = {
                .mv_size = sizeof(*encodedValue),
                .mv_data = const_cast<EncodedValue*>(encodedValue)
            };

            put(txn.txn, self->rootDb, &mdbKey, &mdbValue);
        }
        txn.commit();
    } catch(const OocError& error) {
        if(error.errorCode == OocError::MutableValueNotAllowed)
            PyErr_Format(PyExc_TypeError, "unhashable type: '%s'", Py_TYPE(key)->tp_name);
        else
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

    try {
        OOCTransaction txn(self, true);

        const EncodedValue* const encodedKey = OOCMap_encode(self, key, txn, true, true);
        MDB_val mdbKey = {
            .mv_size = sizeof(*encodedKey),
            .mv_data = const_cast<EncodedValue*>(encodedKey)
        };

        MDB_val mdbValue;
        const bool found = get(txn.txn, self->rootDb, &mdbKey, &mdbValue);
        if(found) {
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* encodedValue = static_cast<EncodedValue*>(mdbValue.mv_data);

            PyObject* const result = OOCMap_decode(self, encodedValue, txn);
            txn.commit();
            return result;
        } else {
            PyErr_SetObject(PyExc_KeyError, key);
            return nullptr;
        }
    } catch(const OocError& error) {
        switch(error.errorCode) {
        case OocError::MutableValueNotAllowed:
            PyErr_Format(PyExc_TypeError, "unhashable type: '%s'", Py_TYPE(key)->tp_name);
            break;
        case OocError::WriteNotAllowed:
        case OocError::ImmutableValueNotFound:
            PyErr_SetObject(PyExc_KeyError, key);
            break;
        default:
            error.pythonize();
            break;
        }
        return nullptr;
    }
}


//
// Python definitions to tie it all together
//

static PyMethodDef OOCMap_methods[] = {
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