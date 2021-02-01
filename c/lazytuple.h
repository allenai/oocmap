#ifndef OOCMAP_LAZYTUPLE_H
#define OOCMAP_LAZYTUPLE_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "oocmap.h"

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;
    uint64_t tupleId;
} OOCLazyTupleObject;

extern PyTypeObject OOCLazyTupleType;

OOCLazyTupleObject* OOCLazyTuple_fastnew(OOCMapObject* ooc, uint64_t tupleId);

#endif
