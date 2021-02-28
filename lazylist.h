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

Py_ssize_t OOCLazyListObject_index(
    OOCLazyListObject* self,
    PyObject* value,
    Py_ssize_t start,
    Py_ssize_t stop,
    MDB_txn* txn);

Py_ssize_t OOCLazyListObject_count(OOCLazyListObject* self, PyObject* value, MDB_txn* txn);
void OOCLazyListObject_extend(OOCLazyListObject* self, PyObject* other, MDB_txn* txn);
void OOCLazyListObject_extend(OOCLazyListObject* self, OOCLazyListObject* other, MDB_txn* txn);
void OOCLazyListObject_append(OOCLazyListObject* self, PyObject* item, MDB_txn* txn);
void OOCLazyListObject_clear(OOCLazyListObject* self, MDB_txn* txn);

//
// OOCLazyListIter
//

typedef struct {
    PyObject_HEAD
    OOCLazyListObject* list;
    MDB_cursor* cursor;
} OOCLazyListIterObject;

extern PyTypeObject OOCLazyListIterType;

OOCLazyListIterObject* OOCLazyListIter_fastnew(OOCLazyListObject* list);


#endif
