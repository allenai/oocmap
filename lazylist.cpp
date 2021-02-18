#include "lazylist.h"

#include "oocmap.h"
#include "db.h"
#include "errors.h"

//
// Methods that are not directly exposed to Python.
// These throw exceptions.
//

OOCLazyListObject* OOCLazyList_fastnew(OOCMapObject* const ooc, const uint32_t listId) {
    PyObject* const pySelf = OOCLazyListType.tp_alloc(&OOCLazyListType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyListObject* self = reinterpret_cast<OOCLazyListObject*>(pySelf);
    self->ooc = ooc;
    Py_INCREF(ooc);
    self->listId = listId;
    return self;
}

OOCLazyListIterObject* OOCLazyListIter_fastnew(OOCLazyListObject* const list) {
    PyObject* const pySelf = OOCLazyListIterType.tp_alloc(&OOCLazyListIterType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyListIterObject* self = reinterpret_cast<OOCLazyListIterObject*>(pySelf);
    self->list = list;
    Py_INCREF(list);
    self->cursor = nullptr;
    return self;
}

//
// Methods that are directly exposed to Python
// These are not allowed to throw exceptions.
//

static PyObject* OOCLazyList_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyListObject* self = reinterpret_cast<OOCLazyListObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->ooc = nullptr;
    self->listId = 0;
    return (PyObject*)self;
}

static PyObject* OOCLazyListIter_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyListIterObject* self = reinterpret_cast<OOCLazyListIterObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->list = nullptr;
    self->cursor = nullptr;
    return (PyObject*)self;
}

static int OOCLazyList_init(OOCLazyListObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"oocmap", "list_id", nullptr};
    PyObject* oocmapObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!K",
        const_cast<char**>(kwlist),
        &OOCMapType, &oocmapObject, &self->listId);
    if(!parseSuccess)
        return -1;

    // TODO: consider that __init__ might be called on an already initialized object
    self->ooc = reinterpret_cast<OOCMapObject*>(oocmapObject);
    Py_INCREF(oocmapObject);

    return 0;
}

static int OOCLazyListIter_init(OOCLazyListIterObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"list", "list_id", nullptr};
    PyObject* listObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!",
        const_cast<char**>(kwlist),
        &OOCLazyListType, &listObject);
    if(!parseSuccess)
        return -1;

    // TODO: consider that __init__ might be called on an already initialized object
    self->list = reinterpret_cast<OOCLazyListObject*>(listObject);
    Py_INCREF(listObject);
    self->cursor = nullptr;

    return 0;
}

static void OOCLazyList_dealloc(OOCLazyListObject* const self) {
    Py_DECREF(self->ooc);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static void OOCLazyListIter_dealloc(OOCLazyListIterObject* const self) {
    Py_XDECREF(self->list);
    if(self->cursor != nullptr) {
        MDB_txn* const txn = mdb_cursor_txn(self->cursor);
        mdb_cursor_close(self->cursor);
        txn_abort(txn);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyList_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        const Py_ssize_t result = OOCLazyListObject_length(self, txn);
        txn_commit(txn);
        return result;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }
}

Py_ssize_t OOCLazyListObject_length(OOCLazyListObject* const self, MDB_txn* const txn) {
    ListKey encodedListKey = {
        .listIndex = std::numeric_limits<uint32_t>::max(),
        .listId = self->listId,
    };

    MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
    MDB_val mdbValue;
    get(txn, self->ooc->listsDb, &mdbKey, &mdbValue);
    if(mdbValue.mv_size != sizeof(Py_ssize_t)) throw OocError(OocError::UnexpectedData);
    return *reinterpret_cast<Py_ssize_t*>(mdbValue.mv_data);
}

static PyObject* OOCLazyList_item(PyObject* const pySelf, Py_ssize_t const index) {
    if(index < 0) {
        // Negative indices are already handled for us. If we get one now, it's an
        // automatic IndexError.
        PyErr_Format(PyExc_IndexError, "list index out of range");
        return nullptr;
    }

    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    ListKey encodedListKey = {
        .listIndex = index,
        .listId = self->listId,
    };

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
        MDB_val mdbValue;
        try {
            get(txn, self->ooc->listsDb, &mdbKey, &mdbValue);
        } catch(const MdbError& error) {
            if(error.mdbErrorCode == MDB_NOTFOUND)
                throw OocError(OocError::IndexError);
            else
                throw;
        }
        if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
        EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
        PyObject* const result = OOCMap_decode(self->ooc, encodedResult, txn);
        txn_commit(txn);
        return result;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyList_eager(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        PyObject* const result = OOCLazyListObject_eager(self, txn);
        txn_commit(txn);
        return result;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyListObject_eager(OOCLazyListObject* const self, MDB_txn* const txn) {
    const Py_ssize_t length = OOCLazyListObject_length(self, txn);
    PyObject* result = nullptr;
    MDB_cursor* cursor = nullptr;
    try {
        result = PyList_New(length);
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        if(length <= 0) return result;
        cursor = cursor_open(txn, self->ooc->listsDb);

        ListKey encodedListKey = {
            .listIndex = 0,
            .listId = self->listId,
        };
        MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
        MDB_val mdbValue;
        cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_KEY);

        while(true) {
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
            PyObject* const item = OOCMap_decode(self->ooc, encodedResult, txn);
            PyList_SET_ITEM(result, encodedListKey.listIndex, item);

            cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listKey = static_cast<ListKey* const>(mdbKey.mv_data);
            if(listKey->listIndex == std::numeric_limits<uint32_t>::max()) break;
            if(listKey->listId != self->listId) break;
            encodedListKey.listIndex = listKey->listIndex;
        }

        cursor_close(cursor);
    } catch(...) {
        if(result != nullptr) Py_DECREF(result);
        if(cursor != nullptr) cursor_close(cursor);
        throw;
    }

    return result;
}

static PyObject* OOCLazyList_index(
    PyObject* const pySelf,
    PyObject *const *const args,
    const Py_ssize_t nargs
) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    // parse parameters
    if(nargs < 1) {
        PyErr_Format(PyExc_TypeError, "index expected at least 1 argument, got 0");
        return nullptr;
    }
    PyObject* const value = args[0];

    Py_ssize_t start = 0;
    if(nargs > 1) {
        start = PyLong_AsSsize_t(args[1]);
        if(PyErr_Occurred()) return nullptr;
    }

    Py_ssize_t stop = 9223372036854775807;
    if(nargs > 2) {
        stop = PyLong_AsSsize_t(args[2]);
        if(PyErr_Occurred()) return nullptr;
    }

    Py_ssize_t index;
    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        index = OOCLazyListObject_index(self, value, start, stop, txn);
        txn_commit(txn);
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return nullptr;
    }

    if(index < 0) {
        PyErr_Format(PyExc_ValueError, "%R is not in list", value);
        return nullptr;
    } else {
        return PyLong_FromSsize_t(index);
    }
}

Py_ssize_t OOCLazyListObject_index(
    OOCLazyListObject* const self,
    PyObject* const value,
    Py_ssize_t start,
    Py_ssize_t stop,
    MDB_txn* const txn
) {
    // unfuck start and stop
    // That behavior in Python is seriously weird and we have to copy it here.
    Py_ssize_t length = -1;
    if(start < 0) {
        length = OOCLazyListObject_length(self, txn);
        start += length;
        if(start < 0)
            start = 0;
    }
    if(stop < 0) {
        if(length < 0)
            length = OOCLazyListObject_length(self, txn);
        stop += length;
    }

    Id2EncodedMap insertedItemsInThisTransaction;
    EncodedValue encodedValue;
    try {
        OOCMap_encode(self->ooc, value, &encodedValue, txn, insertedItemsInThisTransaction, true);
    } catch(const MdbError& e) {
        if(e.mdbErrorCode != EACCES) throw;
        // We tried to write the value in a readonly transaction, so we got the EACCES error. This must
        // mean the value is a mutable value. The only thing we can do is search linearly through the list.
        encodedValue.typeCodeWithLength = 0xff;  // Mark the encoded value as unusable
    } catch(const OocError& e) {
        if(e.errorCode != OocError::ImmutableValueNotFound) throw;
        // Needle is immutable but not inserted into the map, so we know for sure we won't find it.
        return -1;
    }

    // We have an encoded value that's guaranteed to be an immutable value, so we can just search the
    // encoded values directly with no need to decode.

    ListKey encodedListKey = {
        .listIndex = start,
        .listId = self->listId,
    };
    static const uint32_t SEARCH_FAILED = std::numeric_limits<uint32_t>::max();
    MDB_cursor* const cursor = cursor_open(txn, self->ooc->listsDb);
    try {
        MDB_val mdbKey = {.mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey};
        MDB_val mdbValue;
        try {
            cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_KEY);
        } catch(const MdbError& e) {
            if(e.mdbErrorCode == MDB_NOTFOUND)
                encodedListKey.listIndex = SEARCH_FAILED;
            else
                throw;
        }

        while(encodedListKey.listIndex < stop && encodedListKey.listIndex != SEARCH_FAILED) {
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedItem = static_cast<EncodedValue* const>(mdbValue.mv_data);
            if(encodedValue.typeCodeWithLength == 0xff) {
                PyObject* const item = OOCMap_decode(self->ooc, encodedItem, txn);
                if(PyObject_RichCompareBool(value, item, Py_EQ))
                    break;
            } else {
                if(encodedValue == *encodedItem)
                    break;
            }

            try {
                cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
            } catch(const MdbError& e) {
                encodedListKey.listIndex = SEARCH_FAILED;
                break;
            }
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listKey = static_cast<ListKey* const>(mdbKey.mv_data);
            if(listKey->listId != self->listId)
                encodedListKey.listIndex = SEARCH_FAILED;
            else
                encodedListKey.listIndex = listKey->listIndex;
        }

        cursor_close(cursor);
    } catch(...) {
        cursor_close(cursor);
        throw;
    }

    if(encodedListKey.listIndex >= stop || encodedListKey.listIndex == SEARCH_FAILED)
        return -1;
    else
        return encodedListKey.listIndex;
}

static PyObject* OOCLazyList_iter(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);
    return reinterpret_cast<PyObject*>(OOCLazyListIter_fastnew(self));
}

static PyObject* OOCLazyListIter_iter(PyObject* const pySelf) {
    Py_INCREF(pySelf);
    return pySelf;
}

static PyObject* OOCLazyListIter_iternext(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListIterType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListIterObject* const self = reinterpret_cast<OOCLazyListIterObject*>(pySelf);
    if(self->list == nullptr) return nullptr;
    OOCMapObject* const ooc = self->list->ooc;

    if(self->cursor == nullptr) {
        MDB_txn* txn = nullptr;
        try {
            txn = txn_begin(ooc->mdb, false);
            self->cursor = cursor_open(txn, ooc->listsDb);

            ListKey encodedListKey = {
                .listIndex = 0,
                .listId = self->list->listId
            };
            MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
            MDB_val mdbValue;
            try {
                cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_SET_KEY);
            } catch(const MdbError& e) {
                if(e.mdbErrorCode == MDB_NOTFOUND) throw OocError(OocError::IndexError);
                throw;
            }
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
            return OOCMap_decode(ooc, encodedResult, txn);
        } catch(const OocError& error) {
            if(self->cursor != nullptr) {
                cursor_close(self->cursor);
                self->cursor = nullptr;
            }
            if(error.errorCode == OocError::IndexError) {
                txn_commit(txn);
                Py_CLEAR(self->list);
            } else {
                if(txn != nullptr)
                    txn_abort(txn);
                error.pythonize();
            }
            return nullptr;
        }
    } else {
        MDB_txn* txn = mdb_cursor_txn(self->cursor);
        try {
            MDB_val mdbKey;
            MDB_val mdbValue;
            cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_NEXT);
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listKey = static_cast<ListKey* const>(mdbKey.mv_data);
            if(
                listKey->listIndex == std::numeric_limits<uint32_t>::max() ||
                listKey->listId != self->list->listId
            ) {
                throw OocError(OocError::IndexError);
            }
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
            return OOCMap_decode(ooc, encodedResult, txn);
        } catch(const OocError& error) {
            cursor_close(self->cursor);
            self->cursor = nullptr;
            if(error.errorCode == OocError::IndexError) {
                txn_commit(txn);
                Py_CLEAR(self->list);
            } else {
                txn_abort(txn);
                error.pythonize();
            }
            return nullptr;
        }
    }
}

static PyObject* _computeRichcompareResult(const int comparisonResult, const int op) {
    bool result;
    if(comparisonResult == 0) {
        switch(op) {
        case Py_LT:
        case Py_NE:
        case Py_GT:
            result = false;
            break;
        case Py_LE:
        case Py_EQ:
        case Py_GE:
            result = true;
            break;
        default:
            PyErr_BadInternalCall();
            return nullptr;
        }
    } else {
        // Compares as if comparisonResult was -1.
        switch(op) {
        case Py_LT:
        case Py_LE:
        case Py_NE:
            result = true;
            break;
        case Py_GT:
        case Py_EQ:
        case Py_GE:
            result = false;
            break;
        default:
            PyErr_BadInternalCall();
            return nullptr;
        }
        // If comparisonResult wasn't -1, flip our answer.
        if(comparisonResult > 0)
            result = !result;
    }

    if(result) {
        Py_INCREF(Py_True);
        return Py_True;
    } else {
        Py_INCREF(Py_False);
        return Py_False;
    }
}

PyObject* OOCLazyList_richcompare(PyObject* const pySelf, PyObject* const other, const int op) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }

    if(PyList_Check(other) || other->ob_type == &OOCLazyListType) {
        PyObject* const selfIter = PyObject_GetIter(pySelf);
        if(selfIter == nullptr) return nullptr;
        PyObject* const otherIter = PyObject_GetIter(other);
        if(otherIter == nullptr) return nullptr;

        while(true) {
            PyObject* const selfItem = PyIter_Next(selfIter);
            if(PyErr_Occurred()) return nullptr;
            PyObject* const otherItem = PyIter_Next(otherIter);
            if(PyErr_Occurred()) return nullptr;

            if(selfItem == nullptr && otherItem == nullptr) {
                // Both iterators are at the end, and they compared the same all the way though.
                return _computeRichcompareResult(0, op);
            }
            if(selfItem == nullptr) {
                // self is exhausted, but other still has items. Self is a prefix of other.
                return _computeRichcompareResult(-1, op);
            }
            if(otherItem == nullptr) {
                // other is exhausted, but self still has items. Other is a prefix of self.
                return _computeRichcompareResult(1, op);
            }

            const int lessThan = PyObject_RichCompareBool(selfItem, otherItem, Py_LT);
            if(lessThan < 0) return nullptr;
            if(lessThan) return _computeRichcompareResult(-1, op);

            const int greaterThan = PyObject_RichCompareBool(selfItem, otherItem, Py_GT);
            if(greaterThan < 0) return nullptr;
            if(greaterThan) return _computeRichcompareResult(1, op);
        }
    } else {
        switch(op) {
        case Py_EQ:
            Py_INCREF(Py_False);
            return Py_False;
        case Py_NE:
            Py_INCREF(Py_True);
            return Py_True;
        default:
            PyErr_Format(PyExc_TypeError, "Operation not supported between these types");
            return nullptr;
        }
    }
}

static PyMethodDef OOCLazyList_methods[] = {
    {
        "eager",
        (PyCFunction)OOCLazyList_eager,
        METH_NOARGS,
        PyDoc_STR("returns the original list")
    }, {
        "index",
        (PyCFunction)OOCLazyList_index,
        METH_FASTCALL,
        PyDoc_STR("returns the index of the given item in the list")
    },
    {nullptr}, // sentinel
};

static PySequenceMethods OOCLazyList_sequence_methods = {
    .sq_length = OOCLazyList_length,
    .sq_concat = nullptr, // TODO OOCLazyList_concat,
    .sq_repeat = nullptr, // TODO OOCLazyList_repeat,
    .sq_item = OOCLazyList_item,
    .sq_contains = nullptr, // TODO OOCLazyList_contains
};

PyTypeObject OOCLazyListType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyList",
    .tp_basicsize = sizeof(OOCLazyListObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyList_dealloc,
    .tp_as_sequence = &OOCLazyList_sequence_methods,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A list-like class that's backed by an OOCMap",
    .tp_richcompare = OOCLazyList_richcompare,
    .tp_iter = OOCLazyList_iter,
    .tp_methods = OOCLazyList_methods,
    .tp_init = (initproc)OOCLazyList_init,
    .tp_new = OOCLazyList_new,
};

PyTypeObject OOCLazyListIterType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyListIter",
    .tp_basicsize = sizeof(OOCLazyListIterObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyListIter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "An iterator over a LazyList",
    .tp_iter = OOCLazyListIter_iter,
    .tp_iternext = OOCLazyListIter_iternext,
    .tp_init = (initproc)OOCLazyListIter_init,
    .tp_new = OOCLazyListIter_new,
};
