#include "errors.h"

#include "lmdb.h"

void OocError::pythonize() const {
    switch(errorCode) {
    case NoError:
        PyErr_Format(PyExc_ValueError, "Error: There is no error.");
        break;
    case AlreadyPythonizedError:
        break;
    case ImmutableValueNotFound:
        PyErr_Format(PyExc_ValueError, "Tried to write a non-existant immutable value into the DB in a readonly transaction.");
        break;
    case InvalidBool:
        PyErr_Format(PyExc_ValueError, "Found a bool that's neither true nor false.");
        break;
    case CouldNotReadyString:
        PyErr_Format(PyExc_MemoryError, "Could not bring string into the canonical representation.");
        break;
    case InvalidStringKind:
        PyErr_Format(PyExc_ValueError, "Unknown kind of string");
        break;
    case OutOfMemory:
        PyErr_NoMemory();
        break;
    case UnknownType:
        PyErr_Format(PyExc_ValueError, "Tried to serialize or deserialize object of unknown type");
        break;
    case UnknownHardcodedValue:
        PyErr_Format(PyExc_AssertionError, "Unexpected hardcoded value");
        break;
    case UnexpectedData:
        PyErr_Format(PyExc_AssertionError, "Unexpected data in database");
        break;
    case IndexError:
        PyErr_Format(PyExc_IndexError, "index out of range");
        break;
    case MdbError:
        PyErr_Format(PyExc_IOError, "Unknown problem with LMDB");
        break;
    }
}

void UnknownTypeError::pythonize() const {
    PyErr_Format(
        PyExc_ValueError,
        "Cannot serialize objects of type %s",
        PyUnicode_AsUTF8(PyObject_Repr(type)));
}

void MdbError::pythonize() const {
    switch(mdbErrorCode) {
    case 0:
        PyErr_Format(PyExc_ValueError, "Error: There is no error.");
        break;
    case ENOMEM:
        PyErr_NoMemory();
        break;
    case EINVAL:
        PyErr_Format(PyExc_IOError,"LMDB: An invalid parameter was specified.");
        break;
    case ENOSPC:
        PyErr_Format(PyExc_IOError,"LMDB: No more disk space.");
        break;
    case EIO:
        PyErr_Format(PyExc_IOError,"LMDB: A low-level I/O error occurred while writing.");
        break;
    case EACCES:
        PyErr_Format(PyExc_IOError, "LMDB: Access denied");
        break;
    case ENOENT:
        PyErr_Format(PyExc_IOError, "LMDB Error: The directory specified by the path parameter doesn't exist.");
        break;
    case EAGAIN:
        PyErr_Format(PyExc_IOError, "LMDB Error: The environment was locked by another process.");
        break;
    default:
        PyErr_Format(PyExc_IOError, "MDB Error: %s", mdb_strerror(mdbErrorCode));
        break;
    }
}
