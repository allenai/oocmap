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

Py_ssize_t OOCLazyListObject_length(OOCLazyListObject* self, OOCTransaction& txn);

PyObject* OOCLazyListObject_eager(OOCLazyListObject* self, OOCTransaction& txn);
PyObject* OOCLazyList_eager(PyObject* pySelf);

Py_ssize_t OOCLazyListObject_index(
    OOCLazyListObject* self,
    OOCTransaction& txn,
    PyObject* value,
    Py_ssize_t start = 0,
    Py_ssize_t stop = 9223372036854775807
);

Py_ssize_t OOCLazyListObject_count(OOCLazyListObject* self, OOCTransaction& txn, PyObject* value);
void OOCLazyListObject_extend(OOCLazyListObject* self, OOCTransaction& txn, PyObject* pyOther);
void OOCLazyListObject_extend(OOCLazyListObject* self, OOCTransaction& txn, OOCLazyListObject* other);
void OOCLazyListObject_append(OOCLazyListObject* self, OOCTransaction& txn, PyObject* item);
void OOCLazyListObject_clear(OOCLazyListObject* self, OOCTransaction& txn);
void OOCLazyListObject_inplaceRepeat(OOCLazyListObject* self, OOCTransaction& txn, unsigned int count);

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
