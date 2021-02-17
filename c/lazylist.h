#ifndef OOCMAP_LAZYLIST_H
#define OOCMAP_LAZYLIST_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "oocmap.h"
#include "lmdb.h"

//
// OOCLazyList
//

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;
    uint32_t listId;
} OOCLazyListObject;

extern PyTypeObject OOCLazyListType;

OOCLazyListObject* OOCLazyList_fastnew(OOCMapObject* ooc, uint32_t listId);

Py_ssize_t OOCLazyListObject_length(OOCLazyListObject* self, MDB_txn* txn);

PyObject* OOCLazyListObject_eager(OOCLazyListObject* self, MDB_txn* txn);
PyObject* OOCLazyList_eager(PyObject* pySelf);


//
// OOCLazyListIter
//

typedef struct {
    PyObject_HEAD
    OOCMapObject* ooc;  // TODO: just refer to the OOCLazyList instead
    uint32_t listId;
    MDB_cursor* cursor;
} OOCLazyListIterObject;

extern PyTypeObject OOCLazyListIterType;

OOCLazyListIterObject* OOCLazyListIter_fastnew(OOCMapObject* ooc, uint32_t listId);


#endif
