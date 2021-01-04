#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <unordered_map>
#include "lmdb.h"

typedef std::unordered_map<unsigned int, unsigned char[9]> Id2EncodedMap;

typedef struct {
    PyObject_HEAD
    MDB_env* mdb;
    MDB_dbi rootDb;
    MDB_dbi intsDb;
    MDB_dbi stringsDb;
    MDB_dbi listsDb;
    MDB_dbi tuplesDb;
    MDB_dbi dictsDb;
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
        mdb_env_set_maxdbs(self->mdb, 6);
        new(&self->idsWrittenThisTransaction) Id2EncodedMap();  // placement new
    }
    return (PyObject*)self;
}

static MDB_txn* txn_begin(OOCMapObject* const self, const bool write = false) {
    const unsigned int flags = write ? 0 : MDB_RDONLY;
    MDB_txn *txn = nullptr;
    int mapsizePatience = 10;
    while (true) {
        int error = mdb_txn_begin(self->mdb, nullptr, flags, &txn);
        switch (error) {
            case 0:
                return txn;
            case MDB_PANIC:
                PyErr_Format(
                        PyExc_IOError,
                        "LMDB: A fatal error occurred earlier and the environment must be shut down.");
                return nullptr;
            case MDB_MAP_RESIZED:
                if (mapsizePatience > 0) {
                    mapsizePatience -= 1;
                    error = mdb_env_set_mapsize(self->mdb, 0);
                    if (error != 0) {
                        PyErr_Format(
                                PyExc_IOError,
                                "LMDB: Error %d occurred while resizing the memory map.", error);
                        return nullptr;
                    } else {
                        MDB_envinfo info;
                        mdb_env_info(self->mdb, &info);
                        self->currentMapSize = info.me_mapsize;
                        continue;
                    }
                } else {
                    PyErr_Format(
                            PyExc_IOError,
                            "LMDB: Ran out of patience while resizing the memory map.");
                    return nullptr;
                }
            case MDB_READERS_FULL:
                PyErr_Format(
                        PyExc_IOError,
                        "LMDB: A read-only transaction was requested and the reader lock table is full.");
                return nullptr;
            case ENOMEM:
                PyErr_NoMemory();
                return nullptr;
            default:
                PyErr_Format(
                        PyExc_IOError,
                        "Unknown LMDB error %d", error);
                return nullptr;
        }
    }
}

static bool txn_commit(MDB_txn* const txn) {
    const int error = mdb_txn_commit(txn);
    switch (error) {
        case 0:
            return true;
        case EINVAL:
            PyErr_Format(
                    PyExc_IOError,
                    "LMDB: An invalid parameter was specified.");
            return false;
        case ENOSPC:
            PyErr_Format(
                    PyExc_IOError,
                    "LMDB: No more disk space.");
            return false;
        case EIO:
            PyErr_Format(
                    PyExc_IOError,
                    "LMDB: A low-level I/O error occurred while writing.");
            return false;
        case ENOMEM:
            PyErr_NoMemory();
            return false;
        default:
            PyErr_Format(
                    PyExc_IOError,
                    "Unknown LMDB error %d", error);
            return false;
    }
}

static MDB_dbi* open_db(MDB_txn* const txn, const char* const name, unsigned int flags, MDB_dbi* const dbi) {
    const int error = mdb_dbi_open(txn, name, flags | MDB_CREATE, dbi);
    switch(error) {
        case 0:
            return dbi;
        case MDB_NOTFOUND:
            PyErr_Format(
                    PyExc_IOError,
                    "LMDB: The %s database doesn't exist in the environment and MDB_CREATE was not specified.", name);
            return nullptr;
        case MDB_DBS_FULL:
            PyErr_Format(
                    PyExc_IOError,
                    "LMDB: Too many databases have been opened.");
            return nullptr;
        default:
            PyErr_Format(
                    PyExc_IOError,
                    "Unknown LMDB error while opening the %s database: %d", name, error);
            return nullptr;
    }
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
    // TODO: We should check for and handle the case where self->mdb has already been opened.
    // These are some aggressive flags that don't guarantee data integrity.
    const int mdbOpenError = mdb_env_open(
            self->mdb,
            filename,
            MDB_NOSUBDIR | MDB_WRITEMAP | MDB_NOMETASYNC| MDB_MAPASYNC | MDB_NOMEMINIT,
            0644);
    Py_CLEAR(filenameObject);
    switch(mdbOpenError) {
        case 0:
            break;
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
    MDB_envinfo info;
    mdb_env_info(self->mdb, &info);
    self->currentMapSize = info.me_mapsize;

    // open all the DBs
    MDB_txn* const txn = txn_begin(self, true);
    if(txn == nullptr) return -1;
    MDB_dbi* dbi = open_db(txn, "root", MDB_CREATE, &self->rootDb);
    if(dbi != nullptr)
        dbi = open_db(txn, "ints", MDB_CREATE | MDB_INTEGERKEY, &self->intsDb);
    if(dbi != nullptr)
        dbi = open_db(txn, "strings", MDB_CREATE | MDB_INTEGERKEY, &self->stringsDb);
    if(dbi != nullptr)
        dbi = open_db(txn, "lists", MDB_CREATE | MDB_INTEGERKEY, &self->listsDb);
    if(dbi != nullptr)
        dbi = open_db(txn, "tuples", MDB_CREATE | MDB_INTEGERKEY, &self->tuplesDb);
    if(dbi != nullptr)
        dbi = open_db(txn, "dicts", MDB_CREATE, &self->dictsDb);
    if(dbi == nullptr) {
        mdb_txn_abort(txn);
        return -1;
    }
    const int commitError = mdb_txn_commit(txn);
    switch(commitError) {
        case 0:
            break;
        case EINVAL:
            PyErr_Format(PyExc_IOError, "LMDB: An invalid parameter was specified.");
            return -1;
        case ENOSPC:
            PyErr_Format(PyExc_IOError, "LMDB: No more disk space.");
            return -1;
        case EIO:
            PyErr_Format(PyExc_IOError, "LMDB: A low-level I/O error occurred while writing.");
            return -1;
        case ENOMEM:
            PyErr_NoMemory();
            return -1;
        default:
            PyErr_Format(PyExc_IOError, "Unknown LMDB error %d", commitError);
            return -1;
    }

    self->idsWrittenThisTransaction.clear();
    self->transactionCount = 0;

    return 0;
}

// TODO: we might not need this
static PyObject* OOCMap_begin_transaction(OOCMapObject* self, PyObject* unused) {
    if(self->transactionCount <= 0)
        self->idsWrittenThisTransaction.clear();
    self->transactionCount += 1;
    return Py_None;
}

// TODO: we might not need this
static PyObject* OOCMap_end_transaction(OOCMapObject* self, PyObject* unused) {
    if(self->transactionCount <= 0)
        return PyErr_Format(PyExc_ValueError, "No transaction running.");
    self->transactionCount -= 1;
    if(self->transactionCount <= 0)
        self->idsWrittenThisTransaction.clear();
    return Py_None;
}

static Py_ssize_t OOCMap_length(PyObject* self);

static PyMethodDef OOCMap_methods[] = {
        {
            "begin_transaction",
            (PyCFunction)OOCMap_begin_transaction,
            METH_NOARGS,
            PyDoc_STR("start a transaction")
        },{
            "end_transaction",
            (PyCFunction)OOCMap_end_transaction,
            METH_NOARGS,
            PyDoc_STR("end a transaction")
        },
        {nullptr}, // sentinel
};

static PyMappingMethods OOCMap_mapping_methods = {
        .mp_length = OOCMap_length,
        .mp_subscript = nullptr,
        .mp_ass_subscript = nullptr
};

static PyTypeObject OOCMapType = {
        PyVarObject_HEAD_INIT(nullptr, 0)
        .tp_name = "oocmap.OOCMap",
        .tp_basicsize = sizeof(OOCMapObject),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)OOCMap_dealloc,
        .tp_as_mapping = &OOCMap_mapping_methods,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "The out-of-core map",
        .tp_methods = OOCMap_methods,
        .tp_init = (initproc)OOCMap_init,
        .tp_new = OOCMap_new,
};

static PyMethodDef OocmapMethods[] = {
    {nullptr, nullptr, 0, nullptr}        /* Sentinel */
};

static Py_ssize_t OOCMap_length(PyObject* pySelf) {
    if(pySelf->ob_type != &OOCMapType) {
        PyErr_BadArgument();
        return -1;
    }
    const auto self = reinterpret_cast<OOCMapObject*>(pySelf);

    MDB_txn* txn = txn_begin(self, false);
    if(txn == nullptr)
        return -1;
    MDB_stat stat;
    mdb_stat(txn, self->rootDb, &stat);
    if(!txn_commit(txn))
        return -1;
    return stat.ms_entries;
}

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

