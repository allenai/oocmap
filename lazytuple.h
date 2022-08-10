#ifndef OOCMAP_LAZYTUPLE_H
#define OOCMAP_LAZYTUPLE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "oocmap.h"
#include "lmdb.h"

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;
    uint64_t tupleId;
    PyObject* eager;
} OOCLazyTupleObject;

extern PyTypeObject OOCLazyTupleType;

OOCLazyTupleObject* OOCLazyTuple_fastnew(OOCMapObject* ooc, uint64_t tupleId);

Py_ssize_t OOCLazyTupleObject_length(OOCLazyTupleObject* self, OOCTransaction& txn);

PyObject* OOCLazyTupleObject_eager(OOCLazyTupleObject* self, OOCTransaction& txn);
PyObject* OOCLazyTuple_eager(PyObject* pySelf);

Py_ssize_t OOCLazyTupleObject_index(
    OOCLazyTupleObject* self,
    OOCTransaction& txn,
    PyObject* value,
    Py_ssize_t start = 0,
    Py_ssize_t stop = 9223372036854775807
);

#endif
