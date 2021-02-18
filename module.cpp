#include "module.h"

#include "oocmap.h"
#include "lazytuple.h"
#include "lazylist.h"
#include "lazydict.h"

static PyMethodDef OocmapMethods[] = {
    {nullptr, nullptr, 0, nullptr}        /* Sentinel */
};

static struct PyModuleDef oocmap_module = {
    PyModuleDef_HEAD_INIT,
    "oocmap",   /* name of module */
    "A Python dictionary that reads and writes its contents to disk.",
    -1,
    OocmapMethods
};

PyMODINIT_FUNC PyInit_oocmap() {
    if(PyType_Ready(&OOCMapType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyTupleType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyListType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyListIterType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyDictType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyDictItemsType) < 0)
        return nullptr;
    if(PyType_Ready(&OOCLazyDictItemsIterType) < 0)
        return nullptr;

    PyObject* const m = PyModule_Create(&oocmap_module);
    if(m == nullptr)
        return nullptr;

    Py_INCREF(&OOCMapType);
    Py_INCREF(&OOCLazyTupleType);
    Py_INCREF(&OOCLazyListType);
    Py_INCREF(&OOCLazyListIterType);
    Py_INCREF(&OOCLazyDictType);
    Py_INCREF(&OOCLazyDictItemsType);
    Py_INCREF(&OOCLazyDictItemsIterType);
    if(
        PyModule_AddObject(m, "OOCMap", (PyObject*)&OOCMapType) < 0 ||
        PyModule_AddObject(m, "LazyTuple", (PyObject*)&OOCLazyTupleType) < 0 ||
        PyModule_AddObject(m, "LazyList", (PyObject*)&OOCLazyListType) < 0 ||
        PyModule_AddObject(m, "LazyListIter", (PyObject*)&OOCLazyListIterType) < 0 ||
        PyModule_AddObject(m, "LazyDict", (PyObject*)&OOCLazyDictType) < 0 ||
        PyModule_AddObject(m, "LazyDictItems", (PyObject*)&OOCLazyDictItemsType) < 0 ||
        PyModule_AddObject(m, "LazyDictItemsIter", (PyObject*)&OOCLazyDictItemsIterType) < 0
    ) {
        Py_DECREF(&OOCMapType);
        Py_DECREF(&OOCLazyTupleType);
        Py_DECREF(&OOCLazyListType);
        Py_DECREF(&OOCLazyListIterType);
        Py_DECREF(&OOCLazyDictType);
        Py_DECREF(&OOCLazyDictItemsType);
        Py_DECREF(&OOCLazyDictItemsIterType);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}