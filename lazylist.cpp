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

    try {
        OOCTransaction txn(self->ooc, true);
        const Py_ssize_t result = OOCLazyListObject_length(self, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }
}

Py_ssize_t OOCLazyListObject_length(OOCLazyListObject* const self, OOCTransaction& txn) {
    ListKey encodedListKey = {
        .listIndex = ListKey::listIndexLength,
        .listId = self->listId,
    };

    MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->listsDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    if(mdbValue.mv_size != sizeof(uint32_t)) throw OocError(OocError::UnexpectedData);
    return *reinterpret_cast<uint32_t*>(mdbValue.mv_data);
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

    try {
        OOCTransaction txn(self->ooc, true);
        MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
        MDB_val mdbValue;
        const bool found = get(txn.txn, self->ooc->listsDb, &mdbKey, &mdbValue);
        if(!found) throw OocError(OocError::IndexError);
        if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
        EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
        PyObject* const result = OOCMap_decode(self->ooc, encodedResult, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

static int OOCLazyList_setItem(
    PyObject* const pySelf,
    const Py_ssize_t index,
    PyObject* const item
) {
    if(index < 0) {
        // Negative indices are already handled for us. If we get one now, it's an
        // automatic IndexError.
        PyErr_Format(PyExc_IndexError, "list index out of range");
        return -1;
    }

    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    ListKey encodedListKey = {
        .listIndex = index,
        .listId = self->listId,
    };
    OOCTransaction txn(self->ooc, false);
    MDB_cursor* sourceCursor = nullptr;
    MDB_cursor* destCursor = nullptr;
    try {
        if(item == nullptr) {
            // We're deleting the item by moving all items after it forwards by one.
            MDB_val mdbValue;
            destCursor = cursor_open(txn.txn, self->ooc->listsDb);
            MDB_val mdbDestKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
            bool destFound = cursor_get(destCursor, &mdbDestKey, &mdbValue, MDB_SET_KEY);
            if(!destFound) throw OocError(OocError::IndexError);

            sourceCursor = cursor_open(txn.txn, self->ooc->listsDb);
            encodedListKey.listIndex += 1;
            MDB_val mdbSourceKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
            bool sourceFound = cursor_get(sourceCursor, &mdbSourceKey, &mdbValue, MDB_SET_RANGE);

            while(sourceFound) {
                if(mdbSourceKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
                ListKey* const sourceListKey = reinterpret_cast<ListKey*>(mdbSourceKey.mv_data);
                if(
                    sourceListKey->listIndex == ListKey::listIndexLength ||
                    sourceListKey->listId != self->listId
                ) {
                    sourceFound = false;
                    break;
                }

                cursor_put(destCursor, &mdbDestKey, &mdbValue, MDB_CURRENT);

                destFound = cursor_get(destCursor, &mdbDestKey, &mdbValue, MDB_NEXT);
                if(!destFound) throw OocError(OocError::UnexpectedData);  // If we found the source before, we must find it again now.
                sourceFound = cursor_get(sourceCursor, &mdbSourceKey, &mdbValue, MDB_NEXT);
            }

            // sourceCursor now points to the length item, which must be updated
            // destCursor now points to the last item, the one we're about to delete. The index of that
            // item is the new length.
            if(mdbDestKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const destListKey = reinterpret_cast<ListKey*>(mdbDestKey.mv_data);
            mdbValue = (MDB_val) { .mv_size = sizeof(destListKey->listIndex), .mv_data = &destListKey->listIndex };
            cursor_put(sourceCursor, &mdbDestKey, &mdbValue, MDB_CURRENT);
            cursor_close(sourceCursor);

            // destCursor now points to the last item and must be deleted
            cursor_del(destCursor);
            cursor_close(destCursor);
        } else {
            // We're setting the item.
            const Py_ssize_t length = OOCLazyListObject_length(self, txn);
            if(index >= length) throw OocError(OocError::IndexError);

            Id2EncodedMap insertedItems;
            const EncodedValue* const encodedItem = OOCMap_encode(self->ooc, item, txn);
            MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
            MDB_val mdbValue = {
                .mv_size = sizeof(*encodedItem),
                .mv_data = const_cast<EncodedValue*>(encodedItem)
            };
            put(txn.txn, self->ooc->listsDb, &mdbKey, &mdbValue);
        }
        txn.commit();
        return 0;
    } catch(const OocError& error) {
        if(sourceCursor != nullptr) cursor_close(sourceCursor);
        if(destCursor != nullptr) cursor_close(destCursor);
        error.pythonize();
        return -1;
    }
}

PyObject* OOCLazyList_eager(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);
        PyObject* const result = OOCLazyListObject_eager(self, txn);
        txn.commit();
        PyObject_Print(result, stderr, Py_PRINT_RAW);
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyListObject_eager(OOCLazyListObject* const self, OOCTransaction& txn) {
    const Py_ssize_t length = OOCLazyListObject_length(self, txn);
    PyObject* result = nullptr;
    MDB_cursor* cursor = nullptr;
    try {
        result = PyList_New(length);
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        if(length <= 0) return result;
        cursor = cursor_open(txn.txn, self->ooc->listsDb);

        ListKey encodedListKey = {
            .listIndex = 0,
            .listId = self->listId,
        };
        MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
        MDB_val mdbValue;
        bool found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_RANGE);

        while(found) {
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listItemKey = static_cast<ListKey*>(mdbKey.mv_data);
            if(
                listItemKey->listId != self->listId ||
                listItemKey->listIndex == ListKey::listIndexLength
            ) {
                found = false;
                break;
            }
            if(listItemKey->listIndex != encodedListKey.listIndex) throw OocError(OocError::UnexpectedData);
            encodedListKey.listIndex += 1;  // We just do this to check that we're setting all the elements in the list.

            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
            PyObject* const item = OOCMap_decode(self->ooc, encodedResult, txn);
            PyList_SET_ITEM(result, listItemKey->listIndex, item);

            found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
        }
        if(encodedListKey.listIndex != length) throw OocError(OocError::UnexpectedData);  // We didn't set all the values in the list.

        cursor_close(cursor);
    } catch(...) {
        if(cursor != nullptr) cursor_close(cursor);
        if(result != nullptr) Py_DECREF(result);
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
    try {
        OOCTransaction txn(self->ooc, true);
        index = OOCLazyListObject_index(self, txn, value, start, stop);
        txn.commit();
    } catch(const OocError& error) {
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
    OOCLazyListObject* self,
    OOCTransaction& txn,
    PyObject* value,
    Py_ssize_t start,
    Py_ssize_t stop
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
    const EncodedValue* encodedValue = nullptr;
    try {
        encodedValue = OOCMap_encode(self->ooc, value, txn, true, true);
    } catch(const OocError& e) {
        switch(e.errorCode) {
        case OocError::MutableValueNotAllowed:
            // For mutable values, we have to search linearly through the list and compare them all.
            break;
        case OocError::ImmutableValueNotFound:
            // The immutable value isn't in the map yet, so it can't possibly be in the list.
            return -1;
        default:
            throw;
        }
    }

    ListKey encodedListKey = {
        .listIndex = start,
        .listId = self->listId,
    };
    MDB_val mdbKey = {.mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey};
    MDB_val mdbValue;
    MDB_cursor* const cursor = cursor_open(txn.txn, self->ooc->listsDb);
    bool found;
    try {
        found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_RANGE);
        while(found) {
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listItemKey = static_cast<ListKey*>(mdbKey.mv_data);
            if(
                listItemKey->listIndex >= stop ||
                listItemKey->listId != self->listId ||
                listItemKey->listIndex == ListKey::listIndexLength
            ) {
                found = false;
                break;
            }

            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedItem = static_cast<EncodedValue* const>(mdbValue.mv_data);
            if(encodedValue == nullptr) {
                PyObject* const item = OOCMap_decode(self->ooc, encodedItem, txn);
                if(PyObject_RichCompareBool(value, item, Py_EQ))
                    break;
            } else {
                if(*encodedValue == *encodedItem)
                    break;
            }

            found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
        }

        cursor_close(cursor);
    } catch(...) {
        cursor_close(cursor);
        throw;
    }

    if(!found)
        return -1;

    ListKey* const listKey = static_cast<ListKey* const>(mdbKey.mv_data);
    return listKey->listIndex;
}

static PyObject* OOCLazyList_count(
    PyObject* const pySelf,
    PyObject* const value
) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    Py_ssize_t count;
    try {
        OOCTransaction txn(self->ooc, true);
        count = OOCLazyListObject_count(self, txn, value);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    return PyLong_FromSsize_t(count);
}

Py_ssize_t OOCLazyListObject_count(OOCLazyListObject* self, OOCTransaction& txn, PyObject* value) {
    Id2EncodedMap insertedItemsInThisTransaction;
    const EncodedValue* encodedValue = nullptr;
    try {
        encodedValue = OOCMap_encode(self->ooc, value, txn, true, true);
    } catch(const OocError& e) {
        switch(e.errorCode) {
        case OocError::MutableValueNotAllowed:
            // For mutable values, we have to search linearly through the list and compare them all.
            break;
        case OocError::ImmutableValueNotFound:
            // The immutable value isn't in the map yet, so it can't possibly be in the list.
            return 0;
        default:
            throw;
        }
    }

    ListKey encodedListKey = {
        .listIndex = 0,
        .listId = self->listId,
    };
    Py_ssize_t count = 0;
    MDB_cursor* const cursor = cursor_open(txn.txn, self->ooc->listsDb);
    try {
        MDB_val mdbKey = {.mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey};
        MDB_val mdbValue;
        bool found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_RANGE);

        while(found) {
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listItemKey = static_cast<ListKey*>(mdbKey.mv_data);
            if(
                listItemKey->listId != self->listId ||
                listItemKey->listIndex == ListKey::listIndexLength
            ) {
                found = false;
                break;
            }

            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedItem = static_cast<EncodedValue* const>(mdbValue.mv_data);
            if(encodedValue == nullptr) {
                PyObject* const item = OOCMap_decode(self->ooc, encodedItem, txn);
                if(PyObject_RichCompareBool(value, item, Py_EQ))
                    count += 1;
            } else {
                if(*encodedValue == *encodedItem)
                    count += 1;
            }

            found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
        }

        cursor_close(cursor);
    } catch(...) {
        cursor_close(cursor);
        throw;
    }

    return count;
}

static PyObject* OOCLazyList_extend(
    PyObject* const pySelf,
    PyObject* const other
) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);
        OOCLazyListObject_extend(self, txn, other);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    Py_RETURN_NONE;
}

void OOCLazyListObject_extend(OOCLazyListObject* self, OOCTransaction& txn, PyObject* pyOther) {
    if(pyOther->ob_type == &OOCLazyListType) {
        OOCLazyListObject* const other = reinterpret_cast<OOCLazyListObject*>(pyOther);
        OOCLazyListObject_extend(self, txn, other);
    } else {
        PyObject* const iter = PyObject_GetIter(pyOther);
        if(iter == nullptr) throw OocError(OocError::AlreadyPythonizedError);
        try {
            ListKey selfEncodedListKey = {
                .listIndex = OOCLazyListObject_length(self, txn),
                .listId = self->listId
            };
            MDB_val mdbSelfKey = {.mv_size = sizeof(selfEncodedListKey), .mv_data = &selfEncodedListKey};

            PyObject* item;
            while((item = PyIter_Next(iter))) {
                const EncodedValue* const encodedItem = OOCMap_encode(self->ooc, item, txn);
                Py_DECREF(item);
                MDB_val mdbValue = {
                    .mv_size = sizeof(*encodedItem),
                    .mv_data = const_cast<EncodedValue*>(encodedItem)
                };
                put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbValue);
                selfEncodedListKey.listIndex += 1;
            }

            // write the new length
            uint32_t newLength = selfEncodedListKey.listIndex;
            MDB_val mdbLength = {.mv_size = sizeof(newLength), .mv_data = &newLength};
            selfEncodedListKey.listIndex = ListKey::listIndexLength;
            put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbLength);
            Py_DECREF(iter);
        } catch(...) {
            Py_DECREF(iter);
            throw;
        }
    }
}

void OOCLazyListObject_extend(OOCLazyListObject* self, OOCTransaction& txn, OOCLazyListObject* other) {
    if(other->ooc == self->ooc) {
        if(self->listId == other->listId) {
            OOCLazyListObject_inplaceRepeat(self, txn, 2);
            return;
        }

        ListKey selfEncodedListKey = {
            .listIndex = OOCLazyListObject_length(self, txn),
            .listId = self->listId
        };
        ListKey otherEncodedListKey = {
            .listIndex = 0,
            .listId = other->listId
        };
        MDB_val mdbSelfKey = {.mv_size = sizeof(selfEncodedListKey), .mv_data = &selfEncodedListKey};
        MDB_val mdbOtherKey = {.mv_size = sizeof(otherEncodedListKey), .mv_data = &otherEncodedListKey};
        MDB_cursor* const cursor = cursor_open(txn.txn, other->ooc->listsDb);
        try {
            MDB_val mdbValue;
            bool found = cursor_get(cursor, &mdbOtherKey, &mdbValue, MDB_SET_RANGE);
            while(found) {
                if(mdbOtherKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
                ListKey* const listItemKey = static_cast<ListKey*>(mdbOtherKey.mv_data);
                if(
                    listItemKey->listId != other->listId ||
                    listItemKey->listIndex == ListKey::listIndexLength
                ) {
                    break;
                }

                if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
                put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbValue);
                selfEncodedListKey.listIndex += 1;

                found = cursor_get(cursor, &mdbOtherKey, &mdbValue, MDB_NEXT);
            }

            cursor_close(cursor);
        } catch(const MdbError& e) {
            cursor_close(cursor);
            if(e.mdbErrorCode != MDB_NOTFOUND)
                throw;
        } catch(...) {
            cursor_close(cursor);
            throw;
        }

        // write the new length
        uint32_t newLength = selfEncodedListKey.listIndex;
        MDB_val mdbLength = {.mv_size = sizeof(newLength), .mv_data = &newLength};
        selfEncodedListKey.listIndex = ListKey::listIndexLength;
        put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbLength);
    } else {
        PyObject* const eager = OOCLazyList_eager(reinterpret_cast<PyObject* const>(other));
        OOCLazyListObject_extend(self, txn, eager);
    }
}

static PyObject* OOCLazyList_inplaceConcat(PyObject* const pySelf, PyObject* const other) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);
        OOCLazyListObject_extend(self, txn, other);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    Py_INCREF(pySelf);
    return pySelf;
}

static PyObject* OOCLazyList_inplaceRepeat(PyObject* const pySelf, const Py_ssize_t count) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);
        OOCLazyListObject_inplaceRepeat(self, txn, count);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    Py_INCREF(pySelf);
    return pySelf;
}

void OOCLazyListObject_inplaceRepeat(OOCLazyListObject* self, OOCTransaction& txn, unsigned int count) {
    if(count <= 0) {
        OOCLazyListObject_clear(self, txn);
        return;
    }

    const Py_ssize_t length = OOCLazyListObject_length(self, txn);
    if(length <= 0) return;

    /*
     * This is a little bit clever. It reads items at the start of the list, and it writes items at the
     * end of the list. When it has gone through the original list once, it will start reading items
     * that it wrote earlier in the same operation. This is fine. It makes the code easier.
     */

    ListKey destEncodedListKey = {
        .listIndex = length,
        .listId = self->listId
    };
    ListKey sourceEncodedListKey = {
        .listIndex = 0,
        .listId = self->listId
    };
    MDB_val mdbDestKey = {.mv_size = sizeof(destEncodedListKey), .mv_data = &destEncodedListKey};
    MDB_val mdbSourceKey = {.mv_size = sizeof(sourceEncodedListKey), .mv_data = &sourceEncodedListKey};
    MDB_cursor* const cursor = cursor_open(txn.txn, self->ooc->listsDb);
    try {
        MDB_val mdbValue;
        bool found = cursor_get(cursor, &mdbSourceKey, &mdbValue, MDB_SET_RANGE);
        if(!found) throw OocError(OocError::UnexpectedData);
        while(destEncodedListKey.listIndex < length * count) {
            if(mdbSourceKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listItemKey = static_cast<ListKey*>(mdbSourceKey.mv_data);
            if(
                listItemKey->listId != self->listId ||
                listItemKey->listIndex == ListKey::listIndexLength
            ) {
                found = false;
                break;
            }

            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            put(txn.txn, self->ooc->listsDb, &mdbDestKey, &mdbValue);
            destEncodedListKey.listIndex += 1;

            found = cursor_get(cursor, &mdbSourceKey, &mdbValue, MDB_NEXT);
            if(!found) throw OocError(OocError::UnexpectedData);
        }

        cursor_close(cursor);
    } catch(const MdbError& e) {
        cursor_close(cursor);
        if(e.mdbErrorCode != MDB_NOTFOUND)
            throw;
    } catch(...) {
        cursor_close(cursor);
        throw;
    }

    // write the new length
    uint32_t newLength = destEncodedListKey.listIndex;
    MDB_val mdbLength = {.mv_size = sizeof(newLength), .mv_data = &newLength};
    destEncodedListKey.listIndex = ListKey::listIndexLength;
    put(txn.txn, self->ooc->listsDb, &mdbDestKey, &mdbLength);
}

static PyObject* OOCLazyList_append(
    PyObject* const pySelf,
    PyObject* const other
) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);
        OOCLazyListObject_append(self, txn, other);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    Py_RETURN_NONE;
}

void OOCLazyListObject_append(OOCLazyListObject* self, OOCTransaction& txn, PyObject* item) {
    const EncodedValue* const encodedItem = OOCMap_encode(self->ooc, item, txn);

    ListKey selfEncodedListKey = {
        .listIndex = OOCLazyListObject_length(self, txn),
        .listId = self->listId
    };
    MDB_val mdbSelfKey = {.mv_size = sizeof(selfEncodedListKey), .mv_data = &selfEncodedListKey};
    MDB_val mdbValue = {
        .mv_size = sizeof(*encodedItem),
        .mv_data = const_cast<EncodedValue*>(encodedItem)
    };
    put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbValue);

    // write the new length
    uint32_t newLength = selfEncodedListKey.listIndex + 1;
    MDB_val mdbLength = {.mv_size = sizeof(newLength), .mv_data = &newLength};
    selfEncodedListKey.listIndex = ListKey::listIndexLength;
    put(txn.txn, self->ooc->listsDb, &mdbSelfKey, &mdbLength);
}

PyObject* OOCLazyList_clear(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);
        OOCLazyListObject_clear(self, txn);
        txn.commit();
        Py_RETURN_NONE;
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

void OOCLazyListObject_clear(OOCLazyListObject* const self, OOCTransaction& txn) {
    MDB_cursor* cursor = nullptr;
    try {
        cursor = cursor_open(txn.txn, self->ooc->listsDb);

        ListKey encodedListKey = {
            .listIndex = 0,
            .listId = self->listId,
        };
        MDB_val mdbKey = { .mv_size = sizeof(encodedListKey), .mv_data = &encodedListKey };
        MDB_val mdbValue;
        bool found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET_RANGE);

        while(found) {
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listItemKey = static_cast<ListKey*>(mdbKey.mv_data);
            if(
                listItemKey->listId != self->listId ||
                listItemKey->listIndex == ListKey::listIndexLength
            ) {
                found = false;
                break;
            }

            cursor_del(cursor);
            found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
        }

        // cursor is now positioned at the length item, which we overwrite with 0
        static uint32_t zeroLength = 0;
        mdbValue = (MDB_val) { .mv_size = sizeof(zeroLength), .mv_data = &zeroLength };
        cursor_put(cursor, &mdbKey, &mdbValue, MDB_CURRENT);

        cursor_close(cursor);
    } catch(...) {
        if(cursor != nullptr) cursor_close(cursor);
        throw;
    }
}

static int OOCLazyList_contains(PyObject* const pySelf, PyObject* const item) {
    if(pySelf->ob_type != &OOCLazyListType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyListObject* const self = reinterpret_cast<OOCLazyListObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);
        const Py_ssize_t index = OOCLazyListObject_index(self, txn, item);
        txn.commit();
        if(index < 0) return 0; else return 1;
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }
}

PyObject* OOCLazyList_concat(PyObject* pySelf, PyObject* pyOther) {
    PyObject* selfEager = nullptr;
    if(pySelf->ob_type == &OOCLazyListType) {
        selfEager = OOCLazyList_eager(pySelf);
        pySelf = selfEager;
    }

    PyObject* otherEager = nullptr;
    if(pyOther->ob_type == &OOCLazyListType) {
        otherEager = OOCLazyList_eager(pyOther);
        pyOther = otherEager;
    }

    PyObject* result = nullptr;
    if(pySelf != nullptr && pyOther != nullptr)
        result = PySequence_Concat(pySelf, pyOther);
    if(selfEager != nullptr)
        Py_DECREF(selfEager);
    if(otherEager != nullptr)
        Py_DECREF(otherEager);

    return result;
}

PyObject* OOCLazyList_repeat(PyObject* const pySelf, const Py_ssize_t count) {
    PyObject* const eager = OOCLazyList_eager(pySelf);
    if(eager == nullptr) return nullptr;
    PyObject* const result = PySequence_Repeat(eager, count);
    Py_DECREF(eager);
    return result;
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
            const bool found = cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_SET_KEY);
            if(!found) {
                cursor_close(self->cursor);
                self->cursor = nullptr;
                txn_commit(txn);
                Py_CLEAR(self->list);
                return nullptr;
            } else {
                if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
                EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
                OOCTransaction oocTxn(txn, true);
                return OOCMap_decode(ooc, encodedResult, oocTxn);
            }
        } catch(const OocError& error) {
            if(self->cursor != nullptr) {
                cursor_close(self->cursor);
                self->cursor = nullptr;
            }
            if(txn != nullptr)
                txn_abort(txn);
            error.pythonize();
            return nullptr;
        }
    } else {
        MDB_txn* txn = mdb_cursor_txn(self->cursor);
        try {
            MDB_val mdbKey;
            MDB_val mdbValue;
            const bool found = cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_NEXT);
            if(!found) {
                cursor_close(self->cursor);
                self->cursor = nullptr;
                txn_commit(txn);
                Py_CLEAR(self->list);
                return nullptr;
            }
            if(mdbKey.mv_size != sizeof(ListKey)) throw OocError(OocError::UnexpectedData);
            ListKey* const listKey = static_cast<ListKey* const>(mdbKey.mv_data);
            if(
                listKey->listIndex == ListKey::listIndexLength ||
                listKey->listId != self->list->listId
            ) {
                cursor_close(self->cursor);
                self->cursor = nullptr;
                txn_commit(txn);
                Py_CLEAR(self->list);
                return nullptr;
            }
            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
            OOCTransaction oocTxn(txn, true);
            return OOCMap_decode(ooc, encodedResult, oocTxn);
        } catch(const OocError& error) {
            cursor_close(self->cursor);
            self->cursor = nullptr;
            txn_abort(txn);
            error.pythonize();
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

            // We have to check for equality first because inequality isn't defined for everything.
            const int equal = PyObject_RichCompareBool(selfItem, otherItem, Py_EQ);
            if(equal < 0) return nullptr;
            if(equal) continue;

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
            Py_RETURN_FALSE;
        case Py_NE:
            Py_RETURN_TRUE;
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
    }, {
        "count",
        (PyCFunction)OOCLazyList_count,
        METH_O,
        PyDoc_STR("counts how often an item appears in the list")
    }, {
        "extend",
        (PyCFunction)OOCLazyList_extend,
        METH_O,
        PyDoc_STR("appends one list to another")
    }, {
        "append",
        (PyCFunction)OOCLazyList_append,
        METH_O,
        PyDoc_STR("appends one item to the list")
    },
    {
        "clear",
        (PyCFunction)OOCLazyList_clear,
        METH_NOARGS,
        PyDoc_STR("wipes the list")
    },
    {nullptr}, // sentinel
};

static PySequenceMethods OOCLazyList_sequence_methods = {
    .sq_length = OOCLazyList_length,
    .sq_concat = OOCLazyList_concat,
    .sq_repeat = OOCLazyList_repeat,
    .sq_item = OOCLazyList_item,
    .sq_ass_item = OOCLazyList_setItem,
    .sq_contains = OOCLazyList_contains,
    .sq_inplace_concat = OOCLazyList_inplaceConcat,
    .sq_inplace_repeat = OOCLazyList_inplaceRepeat
};

static PyNumberMethods OOCLazyList_number_methods = {
    .nb_add = OOCLazyList_concat
};

PyTypeObject OOCLazyListType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyList",
    .tp_basicsize = sizeof(OOCLazyListObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyList_dealloc,
    .tp_as_number = &OOCLazyList_number_methods,
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
