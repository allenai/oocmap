#include "lazytuple.h"

#include "oocmap.h"
#include "db.h"
#include "errors.h"

//
// Methods that are not directly exposed to Python.
// These throw exceptions.
//

OOCLazyTupleObject* OOCLazyTuple_fastnew(OOCMapObject* const ooc, const uint64_t tupleId) {
    PyObject* const pySelf = OOCLazyTupleType.tp_alloc(&OOCLazyTupleType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyTupleObject* self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);
    self->ooc = ooc;
    Py_INCREF(ooc);
    self->tupleId = tupleId;
    self->eager = nullptr;
    return self;
}

PyObject* OOCLazyTupleObject_eager(OOCLazyTupleObject* const self, OOCTransaction& txn) {
    if(self->eager != nullptr) {
        Py_INCREF(self->eager);
        return self->eager;
    }

    MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    const Py_ssize_t size = mdbValue.mv_size / sizeof(EncodedValue);
    PyObject* const result = PyTuple_New(size);
    if(result == nullptr) throw OocError(OocError::OutOfMemory);
    EncodedValue* const encodedResults = static_cast<EncodedValue* const>(mdbValue.mv_data);
    for(Py_ssize_t i = 0; i < size; ++i)
        PyTuple_SET_ITEM(result, i, OOCMap_decode(self->ooc, encodedResults + i, txn));
    txn.commit();
    self->eager = result;
    Py_INCREF(result);
    return result;
}

//
// Methods that are directly exposed to Python
// These are not allowed to throw exceptions.
//

static PyObject* OOCLazyTuple_new(PyTypeObject* const type, PyObject* const args, PyObject* const kwds) {
    PyObject* pySelf = type->tp_alloc(type, 0);
    OOCLazyTupleObject* self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);
    if(self == nullptr) {
        PyErr_NoMemory();
        return nullptr;
    }
    self->ooc = nullptr;
    self->tupleId = 0;
    self->eager = nullptr;
    return (PyObject*)self;
}

static int OOCLazyTuple_init(OOCLazyTupleObject* const self, PyObject* const args, PyObject* const kwds) {
    // parse parameters
    static const char *kwlist[] = {"oocmap", "tuple_id", nullptr};
    PyObject* oocmapObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
        args,
        kwds,
        "O!K",
        const_cast<char**>(kwlist),
        &OOCMapType, &oocmapObject, &self->tupleId);
    if(!parseSuccess)
        return -1;

    // TODO: consider that __init__ might be called on an already initialized object
    self->ooc = reinterpret_cast<OOCMapObject*>(oocmapObject);
    Py_INCREF(oocmapObject);
    self->eager = nullptr;

    return 0;
}

static void OOCLazyTuple_dealloc(OOCLazyTupleObject* const self) {
    Py_DECREF(self->ooc);
    Py_XDECREF(self->eager);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyTuple_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    if(self->eager != nullptr)
        return PyTuple_Size(self->eager);

    try {
        OOCTransaction txn(self->ooc, true);
        const Py_ssize_t result = OOCLazyTupleObject_length(self, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return -1;
    }
}

Py_ssize_t OOCLazyTupleObject_length(OOCLazyTupleObject* const self, OOCTransaction& txn) {
    if(self->eager != nullptr)
        return PyTuple_Size(self->eager);

    MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    Py_ssize_t result = mdbValue.mv_size / sizeof(EncodedValue);
    return result;
}

static PyObject* OOCLazyTuple_item(PyObject* const pySelf, Py_ssize_t const index) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    if(self->eager != nullptr)
        return PyTuple_GET_ITEM(self->eager, index);

    try {
        OOCTransaction txn(self->ooc, true);
        MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
        MDB_val mdbValue;
        const bool found = get(txn.txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
        if(!found) throw OocError(OocError::UnexpectedData);
        if(index < 0 || index >= static_cast<Py_ssize_t>(mdbValue.mv_size / sizeof(EncodedValue)))
            throw OocError(OocError::IndexError);
        EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data) + index;
        PyObject* const result = OOCMap_decode(self->ooc, encodedResult, txn);
        txn.commit();
        return result;
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

PyObject* OOCLazyTuple_eager(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    if(self->eager != nullptr) {
        Py_INCREF(self->eager);
        return self->eager;
    }

    try {
        OOCTransaction txn(self->ooc, true);
        return OOCLazyTupleObject_eager(self, txn);
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }
}

Py_hash_t OOCLazyTuple_hash(PyObject* const pySelf) {
    // If we want LazyTuple to work as a key in a dict the same way as a normal tuple would, they have to hash
    // to the same thing. The only way to achieve that is to call eager().
    PyObject* const eager = OOCLazyTuple_eager(pySelf);
    if(eager == nullptr) return -1;
    const Py_hash_t result = PyObject_Hash(eager);
    Py_DECREF(eager);
    return result;
}

PyObject* OOCLazyTuple_richcompare(PyObject* const pySelf, PyObject* const other, int op) {
    // TODO: optimize this (but only if it shows up on a profiler)
    PyObject* const eager = OOCLazyTuple_eager(pySelf);
    if(eager == nullptr) return nullptr;
    PyObject* const result = PyObject_RichCompare(eager, other, op);
    Py_DECREF(eager);
    return result;
}

static PyObject* OOCLazyTuple_index(
    PyObject* const pySelf,
    PyObject *const *const args,
    const Py_ssize_t nargs
) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

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
        index = OOCLazyTupleObject_index(self, txn, value, start, stop);
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

Py_ssize_t OOCLazyTupleObject_index(
    OOCLazyTupleObject* self,
    OOCTransaction& txn,
    PyObject* value,
    Py_ssize_t start,
    Py_ssize_t stop
) {
    MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    const Py_ssize_t length = mdbValue.mv_size / sizeof(EncodedValue);
    EncodedValue* const encodedResults = static_cast<EncodedValue* const>(mdbValue.mv_data);

    // unfuck start and stop
    // That behavior in Python is seriously weird and we have to copy it here.
    if(start < 0) {
        start += length;
        if(start < 0)
            start = 0;
    }
    if(stop < 0) {
        stop += length;
    }

    Id2EncodedMap insertedItemsInThisTransaction;
    const EncodedValue* encodedValue = nullptr;
    try {
        const bool oldReadonly = txn.readonly;
        txn.readonly = true;
        encodedValue = OOCMap_encode(self->ooc, value, txn);
        txn.readonly = oldReadonly;
    } catch(const MdbError& e) {
        if(e.mdbErrorCode != EACCES) throw;
        // TODO: This does not really work, because it relies on txn really being readonly, whereas
        // we might just be faking it.

        // We tried to write the value in a readonly transaction, so we got the EACCES error. This must
        // mean the value is a mutable value. The only thing we can do is search linearly through the list.
    } catch(const OocError& e) {
        if(e.errorCode != OocError::ImmutableValueNotFound) throw;
        // Needle is immutable but not inserted into the map, so we know for sure we won't find it.
        return -1;
    }

    while(start < stop) {
        if(start < 0 || start >= length)
            return -1;
        EncodedValue* const encodedItem = encodedResults + start;
        if(encodedValue == nullptr) {
            PyObject* const item = OOCMap_decode(self->ooc, encodedItem, txn);
            if(PyObject_RichCompareBool(value, item, Py_EQ))
                return start;
        } else {
            if(*encodedValue == *encodedItem)
                return start;
        }
        start += 1;
    }
    return -1;
}

static PyObject* OOCLazyTuple_count(
    PyObject* const pySelf,
    PyObject* const value
) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    Py_ssize_t count;
    try {
        OOCTransaction txn(self->ooc, true);
        count = OOCLazyTupleObject_count(self, txn, value);
        txn.commit();
    } catch(const OocError& error) {
        error.pythonize();
        return nullptr;
    }

    return PyLong_FromSsize_t(count);
}

Py_ssize_t OOCLazyTupleObject_count(OOCLazyTupleObject* self, OOCTransaction& txn, PyObject* value) {
    Id2EncodedMap insertedItemsInThisTransaction;
    const EncodedValue* encodedValue = nullptr;
    try {
        const bool oldReadonly = txn.readonly;
        txn.readonly = true;
        encodedValue = OOCMap_encode(self->ooc, value, txn);
        txn.readonly = oldReadonly;
    } catch(const MdbError& e) {
        if(e.mdbErrorCode != EACCES) throw;
        // TODO: This does not really work, because it relies on txn really being readonly, whereas
        // we might just be faking it.

        // We tried to write the value in a readonly transaction, so we got the EACCES error. This must
        // mean the value is a mutable value. The only thing we can do is search linearly through the list.
    } catch(const OocError& e) {
        if(e.errorCode != OocError::ImmutableValueNotFound) throw;
        // Needle is immutable but not inserted into the map, so we know for sure we won't find it.
        return 0;
    }

    MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
    MDB_val mdbValue;
    const bool found = get(txn.txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
    if(!found) throw OocError(OocError::UnexpectedData);
    const Py_ssize_t length = mdbValue.mv_size / sizeof(EncodedValue);
    EncodedValue* const encodedResults = static_cast<EncodedValue* const>(mdbValue.mv_data);

    Py_ssize_t result = 0;
    for(Py_ssize_t i = 0; i < length; ++i) {
        EncodedValue* const encodedItem = encodedResults + i;
        if(encodedValue == nullptr) {
            PyObject* const item = OOCMap_decode(self->ooc, encodedItem, txn);
            if(PyObject_RichCompareBool(value, item, Py_EQ))
                result += 1;
        } else {
            if(*encodedValue == *encodedItem)
                result += 1;
        }
    }
    return result;
}

PyObject* OOCLazyTuple_concat(PyObject* pySelf, PyObject* pyOther) {
    PyObject* selfEager = nullptr;
    if(pySelf->ob_type == &OOCLazyTupleType) {
        selfEager = OOCLazyTuple_eager(pySelf);
        pySelf = selfEager;
    }

    PyObject* otherEager = nullptr;
    if(pyOther->ob_type == &OOCLazyTupleType) {
        otherEager = OOCLazyTuple_eager(pyOther);
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

static PyMethodDef OOCLazyTuple_methods[] = {
    {
        "eager",
        (PyCFunction)OOCLazyTuple_eager,
        METH_NOARGS,
        PyDoc_STR("returns the original tuple")
    }, {
        "index",
        (PyCFunction)OOCLazyTuple_index,
        METH_FASTCALL,
        PyDoc_STR("returns the index of the given item in the tuple")
    }, {
        "count",
        (PyCFunction)OOCLazyTuple_count,
        METH_O,
        PyDoc_STR("counts how often an item appears in the tuple")
    }, {nullptr}, // sentinel
};

static PySequenceMethods OOCLazyTuple_sequence_methods = {
    .sq_length = OOCLazyTuple_length,
    .sq_concat = OOCLazyTuple_concat,
    .sq_repeat = nullptr, // TODO OOCLazyTuple_repeat,
    .sq_item = OOCLazyTuple_item,
    .sq_contains = nullptr, // TODO OOCLazyTuple_contains
};

static PyNumberMethods  OOCLazyTuple_number_methods = {
    .nb_add = OOCLazyTuple_concat,
};

PyTypeObject OOCLazyTupleType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyTuple",
    .tp_basicsize = sizeof(OOCLazyTupleObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyTuple_dealloc,
    .tp_as_number = &OOCLazyTuple_number_methods,
    .tp_as_sequence = &OOCLazyTuple_sequence_methods,
    .tp_hash = OOCLazyTuple_hash,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A tuple-like class that's backed by an OOCMap",
    .tp_richcompare = OOCLazyTuple_richcompare,
    .tp_methods = OOCLazyTuple_methods,
    .tp_init = (initproc)OOCLazyTuple_init,
    .tp_new = OOCLazyTuple_new,
};
