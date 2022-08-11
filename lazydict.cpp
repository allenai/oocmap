#include "lazydict.h"

#include "oocmap.h"
#include "db.h"
#include "errors.h"

//
// OOCLazyDict
//

OOCLazyDictObject* OOCLazyDict_fastnew(OOCMapObject* const ooc, const uint32_t dictId) {
    PyObject* const pySelf = OOCLazyDictType.tp_alloc(&OOCLazyDictType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictObject* self = reinterpret_cast<OOCLazyDictObject*>(pySelf);
    self->ooc = ooc;
    Py_INCREF(ooc);
    self->dictId = dictId;
    return self;
}

static PyObject* OOCLazyDict_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyDictObject* self = reinterpret_cast<OOCLazyDictObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->ooc = nullptr;
    self->dictId = 0;
    return (PyObject*)self;
}

static int OOCLazyDict_init(OOCLazyDictObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"oocmap", "dict_id", nullptr};
    PyObject* oocmapObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!K",
        const_cast<char**>(kwlist),
        &OOCMapType, &oocmapObject, &self->dictId);
    if(!parseSuccess)
        return -1;

    Py_CLEAR(self->ooc);
    self->ooc = reinterpret_cast<OOCMapObject*>(oocmapObject);
    Py_INCREF(oocmapObject);

    return 0;
}

static void OOCLazyDict_dealloc(OOCLazyDictObject* const self) {
    Py_XDECREF(self->ooc);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyDict_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);
        Py_ssize_t const result = OOCLazyDictObject_length(self, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }
}

Py_ssize_t OOCLazyDictObject_length(OOCLazyDictObject* const self, OOCTransaction& txn) {
    MDB_val mdbKey = { .mv_size = sizeof(self->dictId), .mv_data = &self->dictId };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    if(mdbValue.mv_size != sizeof(Py_ssize_t)) throw OocError(OocError::UnexpectedData);
    return *reinterpret_cast<Py_ssize_t*>(mdbValue.mv_data);
}

static int OOCLazyDict_insert(PyObject* pySelf, PyObject* key, PyObject* value) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, false);

        DictItemKey encodedKey = { .dictId = self->dictId };
        try {
            encodedKey.key = *OOCMap_encode(self->ooc, key, txn, true);
        } catch(const OocError& error) {
            switch(error.errorCode) {
            case OocError::MutableValueNotAllowed:
                PyErr_Format(PyExc_TypeError, "unhashable type: '%s'", Py_TYPE(key)->tp_name);
                return -1;
            default:
                throw;
            }
        }

        MDB_val mdbKey = { .mv_size = sizeof(encodedKey), .mv_data = &encodedKey };
        MDB_val mdbValueRead;

        Py_ssize_t lengthChange = 0;
        if(value == nullptr) {
            try {
                del(txn.txn, self->ooc->dictsDb, &mdbKey);
                lengthChange -= 1;
            } catch(const MdbError& error) {
                if(error.mdbErrorCode != MDB_NOTFOUND)
                    throw;
            }
        } else {
            const bool found = get(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValueRead);
            const EncodedValue* const encodedValue = OOCMap_encode(self->ooc, value, txn);
            MDB_val mdbValue = {
                .mv_size = sizeof(*encodedValue),
                .mv_data = const_cast<EncodedValue*>(encodedValue)
            };

            if(found) {
                if(mdbValueRead.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
                const EncodedValue* const encodedValueRead = static_cast<EncodedValue* const>(mdbValueRead.mv_data);
                if(*encodedValue != *encodedValueRead)
                    put(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
            } else {
                put(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
                lengthChange += 1;
            }
        }

        if(lengthChange != 0) {
            Py_ssize_t length = OOCLazyDictObject_length(self, txn) + lengthChange;
            MDB_val mdbLengthKey = {.mv_size = sizeof(self->dictId), .mv_data = &self->dictId};
            MDB_val mdbLengthValue = {.mv_size = sizeof(length), .mv_data = &length};
            put(txn.txn, self->ooc->dictsDb, &mdbLengthKey, &mdbLengthValue);
        }

        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }

    return 0;
}

static PyObject* OOCLazyDict_get(PyObject* const pySelf, PyObject* const key) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);

        DictItemKey encodedItemKey = { .dictId = self->dictId };
        try {
            encodedItemKey.key = *OOCMap_encode(self->ooc, key, txn, true, true);
        } catch(const OocError& error) {
            switch(error.errorCode) {
            case OocError::ImmutableValueNotFound:
            case OocError::WriteNotAllowed:
                PyErr_SetObject(PyExc_KeyError, key);
                return nullptr;
            case OocError::MutableValueNotAllowed:
                PyErr_Format(PyExc_TypeError, "unhashable type: '%s'", Py_TYPE(key)->tp_name);
                return nullptr;
            default:
                throw;
            }
        }

        MDB_val mdbKey = { .mv_size = sizeof(encodedItemKey), .mv_data = &encodedItemKey };
        MDB_val mdbValue;
        const bool found = get(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
        if(!found) throw OocError(OocError::ImmutableValueNotFound);

        if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
        EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data);
        PyObject* const result = OOCMap_decode(self->ooc, encodedResult, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        if(error.errorCode == OocError::ImmutableValueNotFound)
            PyErr_SetObject(PyExc_KeyError, key);
        else
            error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyDict_eager(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);
        return OOCLazyDictObject_eager(self, txn);
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyDictObject_eager(OOCLazyDictObject* const self, OOCTransaction& txn) {
    PyObject* result = nullptr;
    MDB_cursor* cursor = nullptr;
    try {
        result = PyDict_New();
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        cursor = cursor_open(txn.txn, self->ooc->dictsDb);

        MDB_val mdbKey = { .mv_size = sizeof(self->dictId), .mv_data = &self->dictId };
        MDB_val mdbValue;
        bool found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET);
        if(!found) throw OocError(OocError::UnexpectedData);

        while(true) {
            found = cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
            if(!found)
                break;
            if(mdbKey.mv_size != sizeof(DictItemKey)) throw OocError(OocError::UnexpectedData);
            DictItemKey* const encodedItemKey = static_cast<DictItemKey* const>(mdbKey.mv_data);
            if(encodedItemKey->dictId != self->dictId)
                break;
            PyObject* const itemKey = OOCMap_decode(self->ooc, &encodedItemKey->key, txn);

            if(mdbValue.mv_size != sizeof(EncodedValue)) throw OocError(OocError::UnexpectedData);
            EncodedValue* const encodedItemValue = static_cast<EncodedValue* const>(mdbValue.mv_data);
            PyObject* const itemValue = OOCMap_decode(self->ooc, encodedItemValue, txn);

            const int failure = PyDict_SetItem(result, itemKey, itemValue);
            if(failure) throw OocError(OocError::AlreadyPythonizedError);
        }

        cursor_close(cursor);
    } catch(...) {
        if(cursor != nullptr) cursor_close(cursor);
        if(result != nullptr) Py_DECREF(result);
        throw;
    }

    return result;
}

static PyObject* OOCLazyDict_richcompare(PyObject* const pySelf, PyObject* const other, const int op) {
    // TODO: Maybe we can do something faster than using eager? At least something that fails fast
    // when the objects are not equal?
    PyObject* const eager = OOCLazyDict_eager(pySelf);
    if(eager == nullptr) return nullptr;
    PyObject* result = PyObject_RichCompare(eager, other, op);
    Py_DECREF(eager);
    return result;
}

static PyObject* OOCLazyDict_items(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);
    return reinterpret_cast<PyObject*>(OOCLazyDictItems_fastnew(self));
}

static PyObject* OOCLazyDict_iter(PyObject* const pySelf) {
    return PyObject_CallOneArg(reinterpret_cast<PyObject*>(&OOCLazyDictKeysIterType), pySelf);
}

static int OOCLazyDict_contains(PyObject* const pySelf, PyObject* const item) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    try {
        OOCTransaction txn(self->ooc, true);

        DictItemKey encodedItemKey = { .dictId = self->dictId };
        try {
            encodedItemKey.key = *OOCMap_encode(self->ooc, item, txn, true, true);
        } catch(const OocError& error) {
            switch(error.errorCode) {
            case OocError::ImmutableValueNotFound:
            case OocError::WriteNotAllowed:
                return 0;
            case OocError::MutableValueNotAllowed:
                PyErr_Format(PyExc_TypeError, "unhashable type: '%s'", Py_TYPE(item)->tp_name);
                return -1;
            default:
                throw;
            }
        }

        MDB_val mdbKey = { .mv_size = sizeof(encodedItemKey), .mv_data = &encodedItemKey };
        MDB_val mdbValue;
        const bool found = get(txn.txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
        return found ? 1 : 0;
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }
}

static PyMethodDef OOCLazyDict_methods[] = {
    {
        "eager",
        (PyCFunction)OOCLazyDict_eager,
        METH_NOARGS,
        PyDoc_STR("returns the original dict")
    }, {
        "items",
        (PyCFunction)OOCLazyDict_items,
        METH_NOARGS,
        PyDoc_STR("returns a view over the items in the dictionary")
    },
    {nullptr}, // sentinel
};

static PyMappingMethods OOCLazyDict_mapping_methods = {
    .mp_length = OOCLazyDict_length,
    .mp_subscript = OOCLazyDict_get,
    .mp_ass_subscript = OOCLazyDict_insert
};

static PySequenceMethods OOCLazyDict_sequence_methods = {
    .sq_length = OOCLazyDict_length,
    .sq_contains = OOCLazyDict_contains
};

PyTypeObject OOCLazyDictType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyDict",
    .tp_basicsize = sizeof(OOCLazyDictObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyDict_dealloc,
    .tp_as_sequence = &OOCLazyDict_sequence_methods,
    .tp_as_mapping = &OOCLazyDict_mapping_methods,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A dict-like class that's backed by an OOCMap",
    .tp_richcompare = OOCLazyDict_richcompare,
    .tp_iter = OOCLazyDict_iter,
    .tp_methods = OOCLazyDict_methods,
    .tp_init = (initproc)OOCLazyDict_init,
    .tp_new = OOCLazyDict_new,
};


//
// OOCLazyDictItems
//

OOCLazyDictItemsObject* OOCLazyDictItems_fastnew(OOCLazyDictObject* const dict) {
    PyObject* const pySelf = OOCLazyDictItemsType.tp_alloc(&OOCLazyDictItemsType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictItemsObject* self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    self->dict = dict;
    Py_INCREF(dict);
    return self;
}

static PyObject* OOCLazyDictItems_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyDictItemsObject* self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->dict = nullptr;
    return (PyObject*)self;
}

static int OOCLazyDictItems_init(OOCLazyDictItemsObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"dict", nullptr};
    PyObject* dictObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O",
        const_cast<char**>(kwlist),
        &OOCLazyDictType, &dictObject);
    if(!parseSuccess)
        return -1;

    Py_CLEAR(self->dict);
    self->dict = reinterpret_cast<OOCLazyDictObject*>(dictObject);
    Py_INCREF(dictObject);

    return 0;
}

static void OOCLazyDictItems_dealloc(OOCLazyDictItemsObject* const self) {
    Py_XDECREF(self->dict);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyDictItems_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictItemsType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictItemsObject* const self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    return OOCLazyDict_length(reinterpret_cast<PyObject*>(self->dict));
}

static PyObject* OOCLazyDictItems_iter(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictItemsType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictItemsObject* const self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    return reinterpret_cast<PyObject*>(OOCLazyDictItemsIter_fastnew(self->dict));
}

static PySequenceMethods OOCLazyDictItems_sequence_methods = {
    .sq_length = OOCLazyDictItems_length
};

PyTypeObject OOCLazyDictItemsType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyDictItems",
    .tp_basicsize = sizeof(OOCLazyDictItemsObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyDictItems_dealloc,
    .tp_as_sequence = &OOCLazyDictItems_sequence_methods,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "An item view for LazyDict",
    .tp_iter = OOCLazyDictItems_iter,
    .tp_init = (initproc)OOCLazyDictItems_init,
    .tp_new = OOCLazyDictItems_new,
};


//
// OOCLazyDictItemsIter
//

OOCLazyDictItemsIterObject* OOCLazyDictItemsIter_fastnew(OOCLazyDictObject* const dict) {
    PyObject* const pySelf = OOCLazyDictItemsIterType.tp_alloc(&OOCLazyDictItemsIterType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictItemsIterObject* self = reinterpret_cast<OOCLazyDictItemsIterObject*>(pySelf);
    self->dict = dict;
    Py_INCREF(dict);
    self->cursor = nullptr;
    return self;
}

static PyObject* OOCLazyDictItemsIter_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyDictItemsIterObject* self = reinterpret_cast<OOCLazyDictItemsIterObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->dict = nullptr;
    self->cursor = nullptr;
    return (PyObject*)self;
}

static int OOCLazyDictItemsIter_init(OOCLazyDictItemsIterObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"dict", nullptr};
    PyObject* dictObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!",
        const_cast<char**>(kwlist),
        &OOCLazyDictType, &dictObject);
    if(!parseSuccess)
        return -1;

    Py_CLEAR(self->dict);
    self->dict = reinterpret_cast<OOCLazyDictObject*>(dictObject);
    Py_INCREF(dictObject);
    self->cursor = nullptr;

    return 0;
}

static void OOCLazyDictItemsIter_dealloc(OOCLazyDictItemsIterObject* const self) {
    Py_XDECREF(self->dict);
    if(self->cursor != nullptr) {
        MDB_txn* const txn = mdb_cursor_txn(self->cursor);
        mdb_cursor_close(self->cursor);
        txn_abort(txn);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* OOCLazyDictItemsIter_iter(PyObject* const pySelf) {
    Py_INCREF(pySelf);
    return pySelf;
}

static PyObject* OOCLazyDictKeysIter_iter(PyObject* const pySelf) {
    Py_INCREF(pySelf);
    return pySelf;
}

static PyObject* OOCLazyDictItemsIter_iternext(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictItemsIterType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictItemsIterObject* const self = reinterpret_cast<OOCLazyDictItemsIterObject*>(pySelf);
    if(self->dict == nullptr) return nullptr;
    OOCMapObject* const ooc = self->dict->ooc;

    MDB_txn* txn = nullptr;
    if(self->cursor == nullptr) {
        try {
            txn = txn_begin(ooc->mdb, false);
            self->cursor = cursor_open(txn, ooc->dictsDb);

            MDB_val mdbKey = { .mv_size = sizeof(self->dict->dictId), .mv_data = &self->dict->dictId };
            MDB_val mdbValue;
            const bool found = cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_SET);
            if(!found) throw OocError(OocError::UnexpectedData);
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
        txn = mdb_cursor_txn(self->cursor);
    }
    assert(txn != nullptr);

    PyObject* pyKey = nullptr;
    PyObject* pyValue = nullptr;
    try {
        MDB_val mdbKey;
        MDB_val mdbValue;
        const bool found = cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_NEXT);
        if(!found) throw OocError(OocError::IndexError);

        switch(mdbKey.mv_size) {
        case sizeof(DictItemKey):
            break;
        case sizeof(self->dict->dictId):
            throw OocError(OocError::IndexError);
        default:
            throw OocError(OocError::UnexpectedData);
        }
        DictItemKey* const dictItemKey = static_cast<DictItemKey* const>(mdbKey.mv_data);

        if(mdbValue.mv_size != sizeof(EncodedValue))
            throw OocError(OocError::UnexpectedData);
        EncodedValue* const dictItemValue = static_cast<EncodedValue* const>(mdbValue.mv_data);

        OOCTransaction oocTxn(txn, true);
        pyKey = OOCMap_decode(ooc, &dictItemKey->key, oocTxn);
        pyValue = OOCMap_decode(ooc, dictItemValue, oocTxn);
    } catch(const OocError& error) {
        cursor_close(self->cursor);
        self->cursor = nullptr;
        if(error.errorCode == OocError::IndexError) {
            txn_commit(txn);
            Py_CLEAR(self->dict);
        } else {
            txn_abort(txn);
            error.pythonize();
        }
        return nullptr;
    }

    PyObject* const result = PyTuple_New(2);
    if(result == nullptr) {
        Py_DECREF(pyKey);
        Py_DECREF(pyValue);
    }
    PyTuple_SET_ITEM(result, 0, pyKey);
    PyTuple_SET_ITEM(result, 1, pyValue);
    return result;
}

PyTypeObject OOCLazyDictItemsIterType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyDictItemsIter",
    .tp_basicsize = sizeof(OOCLazyDictItemsIterObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyDictItemsIter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "An iterator for the item view for LazyDict",
    .tp_iter = OOCLazyDictItemsIter_iter,
    .tp_iternext = OOCLazyDictItemsIter_iternext,
    .tp_init = (initproc)OOCLazyDictItemsIter_init,
    .tp_new = OOCLazyDictItemsIter_new,
};


//
// OOCLazyDictKeys
//

//
// OOCLazyDictKeysIter
//

OOCLazyDictKeysIterObject* OOCLazyDictKeysIter_fastnew(PyObject* const itemsIter) {
    PyObject* const pySelf = OOCLazyDictKeysIterType.tp_alloc(&OOCLazyDictKeysIterType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictKeysIterObject* self = reinterpret_cast<OOCLazyDictKeysIterObject*>(pySelf);
    self->itemsIter = itemsIter;
    Py_INCREF(itemsIter);
    return self;
}

static PyObject* OOCLazyDictKeysIter_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyDictKeysIterObject* self = reinterpret_cast<OOCLazyDictKeysIterObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->itemsIter = nullptr;
    return (PyObject*)self;
}

static int OOCLazyDictKeysIter_init(OOCLazyDictKeysIterObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"dict", nullptr};
    PyObject* dictObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!",
        const_cast<char**>(kwlist),
        &OOCLazyDictType, &dictObject);
    if(!parseSuccess)
        return -1;

    PyObject* const itemsIter = PyObject_CallOneArg(reinterpret_cast<PyObject*>(&OOCLazyDictItemsIterType), dictObject);
    if(itemsIter == nullptr)
        return -1;

    Py_CLEAR(self->itemsIter);
    self->itemsIter = itemsIter;

    return 0;
}

static void OOCLazyDictKeysIter_dealloc(OOCLazyDictKeysIterObject* const self) {
    Py_XDECREF(self->itemsIter);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* OOCLazyDictKeysIter_iternext(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictKeysIterType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictKeysIterObject* const self = reinterpret_cast<OOCLazyDictKeysIterObject*>(pySelf);

    PyObject* const keyValueTuple = OOCLazyDictItemsIter_iternext(self->itemsIter);
    if(keyValueTuple == nullptr) return nullptr;

    PyObject* const key = PyTuple_GetItem(keyValueTuple, 0);
    if(key == nullptr) return nullptr;

    Py_INCREF(key);
    Py_DECREF(keyValueTuple);
    return key;
}

PyTypeObject OOCLazyDictKeysIterType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyDictKeysIter",
    .tp_basicsize = sizeof(OOCLazyDictKeysIterObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyDictKeysIter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "An iterator for the keys in a LazyDict",
    .tp_iter = OOCLazyDictKeysIter_iter,
    .tp_iternext = OOCLazyDictKeysIter_iternext,
    .tp_init = (initproc)OOCLazyDictKeysIter_init,
    .tp_new = OOCLazyDictKeysIter_new,
};
