#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject* oocmap_test(PyObject* const self, PyObject* const args) {
    Py_RETURN_NONE;
}

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
} OOCMapObject;

static PyTypeObject OOCMapType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "oocmap.OOCMap",
    .tp_doc = "The out-of-core map",
    .tp_basicsize = sizeof(OOCMapObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
};

static PyMethodDef OocmapMethods[] = {
    {"test", oocmap_test, METH_VARARGS, "Make sure the extension module works"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef oocmap_module = {
    PyModuleDef_HEAD_INIT,
    "oocmap",   /* name of module */
    "A Python dictionary that reads and writes its contents to disk.",
    -1,
    OocmapMethods
};

PyMODINIT_FUNC PyInit_oocmap(void) {
    if(PyType_Ready(&OOCMapType) < 0)
        return NULL;

    PyObject* const m = PyModule_Create(&oocmap_module);
    if(m == NULL)
        return NULL;

    Py_INCREF(&OOCMapType);
    if(PyModule_AddObject(m, "OOCMap", (PyObject*)&OOCMapType) < 0) {
        Py_DECREF(&OOCMapType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}

