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

Py_ssize_t OOCLazyDictObject_length(OOCLazyDictObject* self, OOCTransaction& txn);
PyObject* OOCLazyDictObject_eager(OOCLazyDictObject* self, OOCTransaction& txn);
PyObject* OOCLazyDict_eager(PyObject* pySelf);


//
// OOCLazyDictItems
//

typedef struct {
    PyObject_HEAD
    OOCLazyDictObject* dict;
} OOCLazyDictItemsObject;

extern PyTypeObject OOCLazyDictItemsType;

OOCLazyDictItemsObject* OOCLazyDictItems_fastnew(OOCLazyDictObject* dict);


//
// OOCLazyDictItemsIter
//

typedef struct {
    PyObject_HEAD
    OOCLazyDictObject* dict;
    MDB_cursor* cursor;
} OOCLazyDictItemsIterObject;

extern PyTypeObject OOCLazyDictItemsIterType;

OOCLazyDictItemsIterObject* OOCLazyDictItemsIter_fastnew(OOCLazyDictObject* dict);


//
// OOCLazyDictKeys
//

typedef struct {
    PyObject_HEAD
    OOCLazyDictObject* dict;
} OOCLazyDictKeysObject;

extern PyTypeObject OOCLazyDictKeysType;

OOCLazyDictKeysObject* OOCLazyDictKeys_fastnew(OOCLazyDictObject* dict);


//
// OOCLazyDictKeysIter
//

typedef struct {
    PyObject_HEAD
    PyObject* itemsIter;
} OOCLazyDictKeysIterObject;

extern PyTypeObject OOCLazyDictKeysIterType;

OOCLazyDictKeysIterObject* OOCLazyDictKeysIter_fastnew(OOCLazyDictObject* dict);


#endif
