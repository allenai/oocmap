#ifndef OOCMAP_LAZYDICT_H
#define OOCMAP_LAZYDICT_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "oocmap.h"
#include "lmdb.h"

//
// OOCLazyDict
//

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;
    uint32_t dictId;
} OOCLazyDictObject;

extern PyTypeObject OOCLazyDictType;

OOCLazyDictObject* OOCLazyDict_fastnew(OOCMapObject* ooc, uint32_t dictId);

Py_ssize_t OOCLazyDictObject_length(OOCLazyDictObject* self, MDB_txn* txn);

PyObject* OOCLazyDictObject_eager(OOCLazyDictObject* self, MDB_txn* txn);
PyObject* OOCLazyDict_eager(PyObject* pySelf);


//
// OOCLazyDictIter
//

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;
    uint32_t dictId;
    MDB_cursor* cursor;
} OOCLazyDictIterObject;

extern PyTypeObject OOCLazyDictIterType;

OOCLazyDictIterObject* OOCLazyDictIter_fastnew(OOCMapObject* ooc, uint32_t dictId);


#endif
