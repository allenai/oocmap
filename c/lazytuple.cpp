#include "lazytuple.h"

#include "oocmap.h"
#include "db.h"
#include "errors.h"

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

    self->ooc = reinterpret_cast<OOCMapObject*>(oocmapObject);
    Py_INCREF(oocmapObject);

    return 0;
}

OOCLazyTupleObject* OOCLazyTuple_fastnew(OOCMapObject* const ooc, const uint64_t tupleId) {
    PyObject* const pySelf = OOCLazyTupleType.tp_alloc(&OOCLazyTupleType, 0);
    if(pySelf == nullptr) throw OocError(OocError::OutOfMemory);
    OOCLazyTupleObject* self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);
    self->ooc = ooc;
    Py_INCREF(ooc);
    self->tupleId = tupleId;
    return self;
}

static void OOCLazyTuple_dealloc(OOCLazyTupleObject* const self) {
    Py_DECREF(self->ooc);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static Py_ssize_t OOCLazyTuple_length(PyObject* const pySelf) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return -1;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
        MDB_val mdbValue;
        get(txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
        Py_ssize_t result = mdbValue.mv_size / sizeof(EncodedValue);
        txn_commit(txn);
        return result;
    } catch(const OocError& error) {
        if(txn != nullptr)
            txn_abort(txn);
        error.pythonize();
        return -1;
    }
}

static PyObject* OOCLazyTuple_item(PyObject* const pySelf, Py_ssize_t const index) {
    if(pySelf->ob_type != &OOCLazyTupleType) {
        PyErr_BadArgument();
        return nullptr;
    }
    OOCLazyTupleObject* const self = reinterpret_cast<OOCLazyTupleObject*>(pySelf);

    MDB_txn* txn = nullptr;
    try {
        txn = txn_begin(self->ooc->mdb, false);
        MDB_val mdbKey = { .mv_size = sizeof(self->tupleId), .mv_data = &self->tupleId };
        MDB_val mdbValue;
        get(txn, self->ooc->tuplesDb, &mdbKey, &mdbValue);
        if(index < 0 || index > mdbValue.mv_size / sizeof(EncodedValue))
            throw OocError(OocError::IndexError);
        EncodedValue* const encodedResult = static_cast<EncodedValue* const>(mdbValue.mv_data) + index;
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

static PyMethodDef OOCLazyTuple_methods[] = {
    {nullptr}, // sentinel
};

static PySequenceMethods OOCLazyTuple_sequence_methods = {
    .sq_length = OOCLazyTuple_length,
    .sq_concat = nullptr, // TODO OOCLazyTuple_concat,
    .sq_repeat = nullptr, // TODO OOCLazyTuple_repeat,
    .sq_item = OOCLazyTuple_item,
    .sq_contains = nullptr, // TODO OOCLazyTuple_contains
};

PyTypeObject OOCLazyTupleType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "oocmap.LazyTuple",
    .tp_basicsize = sizeof(OOCLazyTupleObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)OOCLazyTuple_dealloc,
    .tp_as_sequence = &OOCLazyTuple_sequence_methods,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "A tuple-like class that's backed by an OOCMap",
    .tp_methods = OOCLazyTuple_methods,
    .tp_init = (initproc)OOCLazyTuple_init,
    .tp_new = OOCLazyTuple_new,
};
