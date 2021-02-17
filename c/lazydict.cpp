#include "lazydict.h"

#include "oocmap.h"
#include "db.h"
#include "errors.h"

//
// Methods that are not directly exposed to Python.
// These throw exceptions.
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

OOCLazyDictItemsObject* OOCLazyDictItems_fastnew(OOCLazyDictObject* const dict) {
    PyObject* const pySelf = OOCLazyDictItemsType.tp_alloc(&OOCLazyDictItemsType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictItemsObject* self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    self->dict = dict;
    Py_INCREF(dict);
    return self;
}

OOCLazyDictItemsIterObject* OOCLazyDictItemsIter_fastnew(OOCLazyDictObject* const dict) {
    PyObject* const pySelf = OOCLazyDictItemsIterType.tp_alloc(&OOCLazyDictItemsIterType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyDictItemsIterObject* self = reinterpret_cast<OOCLazyDictItemsIterObject*>(pySelf);
    self->dict = dict;
    self->cursor = nullptr;
    Py_INCREF(dict);
    return self;
}


//
// Methods that are directly exposed to Python
// These are not allowed to throw exceptions.
//

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

    // TODO: consider that __init__ might be called on an already initialized object
    self->ooc = reinterpret_cast<OOCMapObject*>(oocmapObject);
    Py_INCREF(oocmapObject);

    return 0;
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

    // TODO: consider that __init__ might be called on an already initialized object
    self->dict = reinterpret_cast<OOCLazyDictObject*>(dictObject);
    Py_INCREF(dictObject);

    return 0;
}

static int OOCLazyDictItemsIter_init(OOCLazyDictItemsIterObject* const self, PyObject* const args, PyObject* const kwds) {
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

    // TODO: consider that __init__ might be called on an already initialized object
    self->dict = reinterpret_cast<OOCLazyDictObject*>(dictObject);
    Py_INCREF(dictObject);
    self->cursor = nullptr;

    return 0;
}

static void OOCLazyDict_dealloc(OOCLazyDictObject* const self) {
    Py_DECREF(self->ooc);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static void OOCLazyDictItems_dealloc(OOCLazyDictItemsObject* const self) {
    Py_DECREF(self->dict);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static void OOCLazyDictItemsIter_dealloc(OOCLazyDictItemsIterObject* const self) {
    Py_DECREF(self->dict);
    if(self->cursor != nullptr) {
        MDB_txn* const txn = mdb_cursor_txn(self->cursor);
        mdb_cursor_close(self->cursor);
        txn_abort(txn);
    }
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyDict_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        Py_ssize_t const result = OOCLazyDictObject_length(self, txn);
        txn_commit(txn);
        return result;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }
}

Py_ssize_t OOCLazyDictObject_length(OOCLazyDictObject* const self, MDB_txn* const txn) {
    MDB_val mdbKey = { .mv_size = sizeof(self->dictId), .mv_data = &self->dictId };
    MDB_val mdbValue;
    get(txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
    if(mdbValue.mv_size != sizeof(Py_ssize_t)) throw OocError(OocError::UnexpectedData);
    return *reinterpret_cast<Py_ssize_t*>(mdbValue.mv_data);
}

static Py_ssize_t OOCLazyDictItems_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictItemsType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictItemsObject* const self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    return OOCLazyDict_length(reinterpret_cast<PyObject*>(self->dict));
}

static int OOCLazyDict_insert(PyObject* pySelf, PyObject* key, PyObject* value) {
    if(pySelf->ob_type != &OOCLazyDictType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyDictObject* const self = reinterpret_cast<OOCLazyDictObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        Id2EncodedMap insertedItemsInThisTransaction;
        txn = txn_begin(self->ooc->mdb, true);

        DictItemKey encodedKey = { .dictId = self->dictId };
        OOCMap_encode(self->ooc, key, &encodedKey.key, txn, insertedItemsInThisTransaction);
        EncodedValue encodedValue;
        OOCMap_encode(self->ooc, key, &encodedValue, txn, insertedItemsInThisTransaction);

        MDB_val mdbKey = { .mv_size = sizeof(encodedKey), .mv_data = &encodedKey };
        MDB_val mdbValue = { .mv_size = sizeof(encodedValue), .mv_data = &encodedValue };
        put(txn, self->ooc->dictsDb, &mdbKey, &mdbValue);

        txn_commit(txn);
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
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

    MDB_txn* txn = nullptr;
    try {
        Id2EncodedMap insertedItemsInThisTransaction;
        txn = txn_begin(self->ooc->mdb, false);

        DictItemKey encodedItemKey = { .dictId = self->dictId };
        OOCMap_encode(self->ooc, key, &encodedItemKey.key, txn, insertedItemsInThisTransaction, true);

        MDB_val mdbKey = { .mv_size = sizeof(encodedItemKey), .mv_data = &encodedItemKey };
        MDB_val mdbValue;
        try {
            get(txn, self->ooc->dictsDb, &mdbKey, &mdbValue);
        } catch(const MdbError& e) {
            if(e.mdbErrorCode == MDB_NOTFOUND)
                throw OocError(OocError::ImmutableValueNotFound);
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

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        return OOCLazyDictObject_eager(self, txn);
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyDictObject_eager(OOCLazyDictObject* const self, MDB_txn* const txn) {
    PyObject* result = nullptr;
    MDB_cursor* cursor = nullptr;
    try {
        result = PyDict_New();
        if(result == nullptr) throw OocError(OocError::OutOfMemory);
        cursor = cursor_open(txn, self->ooc->dictsDb);

        MDB_val mdbKey = { .mv_size = sizeof(self->dictId), .mv_data = &self->dictId };
        MDB_val mdbValue;
        cursor_get(cursor, &mdbKey, &mdbValue, MDB_SET);

        while(true) {
            cursor_get(cursor, &mdbKey, &mdbValue, MDB_NEXT);
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
        if(result != nullptr) Py_DECREF(result);
        if(cursor != nullptr) cursor_close(cursor);
        throw;
    }

    return result;
}

static PyObject* OOCLazyDictItems_iter(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyDictItemsType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyDictItemsObject* const self = reinterpret_cast<OOCLazyDictItemsObject*>(pySelf);
    return reinterpret_cast<PyObject*>(OOCLazyDictItemsIter_fastnew(self->dict));
}

static PyObject* OOCLazyDictItemsIter_iter(PyObject* const pySelf) {
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
            cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_SET);
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
        cursor_get(self->cursor, &mdbKey, &mdbValue, MDB_NEXT);

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

        pyKey = OOCMap_decode(ooc, &dictItemKey->key, txn);
        pyValue = OOCMap_decode(ooc, dictItemValue, txn);
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


static PyMethodDef OOCLazyDict_methods[] = {
    {
        "eager",
        (PyCFunction)OOCLazyDict_eager,
        METH_NOARGS,
        PyDoc_STR("returns the original dict")
    },
    {nullptr}, // sentinel
};

static PyMappingMethods OOCLazyDict_mapping_methods = {
    .mp_length = OOCLazyDict_length,
    .mp_subscript = OOCLazyDict_get,
    .mp_ass_subscript = OOCLazyDict_insert
};

PyTypeObject OOCLazyDictType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyDict",
    .tp_basicsize = sizeof(OOCLazyDictObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyDict_dealloc,
    .tp_as_mapping = &OOCLazyDict_mapping_methods,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A dict-like class that's backed by an OOCMap",
    .tp_iter = nullptr,  // TODO?
    .tp_methods = OOCLazyDict_methods,
    .tp_init = (initproc)OOCLazyDict_init,
    .tp_new = OOCLazyDict_new,
};

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
