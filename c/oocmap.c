#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject* oocmap_test(PyObject* const self, PyObject* const args) {
    Py_RETURN_NONE;
}

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
    return PyModule_Create(&oocmap_module);
}

