#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <unordered_map>
#include "lmdb.h"

static PyObject* oocmap_test(PyObject* const self, PyObject* const args) {
    Py_RETURN_NONE;
}

typedef std::unordered_map<unsigned int, unsigned char[9]> Id2EncodedMap;

typedef struct {
    PyObject_HEAD
    MDB_env* mdb;
    size_t currentMapSize;
    Id2EncodedMap idsWrittenThisTransaction;
    unsigned int transactionCount;
} OOCMapObject;

static void OOCMap_dealloc(OOCMapObject* self) {
    mdb_env_close(self->mdb);
    (&self->idsWrittenThisTransaction)->~Id2EncodedMap();   // explicit destructor because this was created with placement new
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* OOCMap_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
    OOCMapObject* self = (OOCMapObject*)type->tp_alloc(type, 0);
    if(self != nullptr) {
        mdb_env_create(&self->mdb);
        new(&self->idsWrittenThisTransaction) Id2EncodedMap();  // placement new
    }
    return (PyObject*)self;
}

static int OOCMap_init(OOCMapObject* self, PyObject* args, PyObject* kwds) {
    // parse parameters
    static const char *kwlist[] = {"filename", nullptr};
    PyObject* filenameObject = nullptr;
    const int parseSuccess = PyArg_ParseTupleAndKeywords(
            args,
            kwds,
            "O&",
            const_cast<char**>(kwlist),
            PyUnicode_FSConverter, &filenameObject);
    if(!parseSuccess)
        return -1;
    const char* filename = PyBytes_AS_STRING(filenameObject);

    // open lmdb
    // These are some aggressive flags that don't guarantee data integrity.
    const int mdbOpenError = mdb_env_open(
            self->mdb,
            filename,
            MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC| MDB_MAPASYNC | MDB_NOMEMINIT,
            0644);
    if(mdbOpenError != 0) {
        Py_CLEAR(filenameObject);
        switch(mdbOpenError) {
            case MDB_VERSION_MISMATCH:
                PyErr_Format(PyExc_IOError, "LMDB Error: The version of the LMDB library doesn't match the version that created the database environment.");
                return -1;
            case MDB_INVALID:
                PyErr_Format(PyExc_IOError, "LMDB Error: The environment file headers are corrupted.");
                return -1;
            case ENOENT:
                PyErr_Format(PyExc_IOError, "LMDB Error: The directory specified by the path parameter doesn't exist.");
                return -1;
            case EACCES:
                PyErr_Format(PyExc_IOError, "LMDB Error: The user didn't have permission to access the environment files.");
                return -1;
            case EAGAIN:
                PyErr_Format(PyExc_IOError, "LMDB Error: The environment was locked by another process.");
                return -1;
            default:
                PyErr_Format(PyExc_IOError, "Unknown LMDB error %d", mdbOpenError);
                return -1;
        }
    }
    MDB_envinfo info;
    mdb_env_info(self->mdb, &info);
    self->currentMapSize = info.me_mapsize;

    self->transactionCount = 0;

    // cleanup
    Py_XDECREF(filenameObject);
    return 0;
}

static PyTypeObject OOCMapType = {
        PyVarObject_HEAD_INIT(nullptr, 0)
        .tp_name = "oocmap.OOCMap",
        .tp_basicsize = sizeof(OOCMapObject),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)OOCMap_dealloc,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "The out-of-core map",
        .tp_init = (initproc)OOCMap_init,
        .tp_new = OOCMap_new,
};

static PyMethodDef OocmapMethods[] = {
    {"test", oocmap_test, METH_VARARGS, "Make sure the extension module works"},
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

    PyObject* const m = PyModule_Create(&oocmap_module);
    if(m == nullptr)
        return nullptr;

    Py_INCREF(&OOCMapType);
    if(PyModule_AddObject(m, "OOCMap", (PyObject*)&OOCMapType) < 0) {
        Py_DECREF(&OOCMapType);
        Py_DECREF(m);
        return nullptr;
    }

    return m;
}

