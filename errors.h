#ifndef OOCMAP_ERRORS_H
#define OOCMAP_ERRORS_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <exception>

struct OocError : std::exception {
    const enum ErrorCode {
        NoError,
        AlreadyPythonizedError, // thrown when Python has already set the error state
        ImmutableValueNotFound,
        InvalidBool,
        CouldNotReadyString,
        InvalidStringKind,
        OutOfMemory,
        UnknownType,
        UnknownHardcodedValue,
        UnexpectedData,
        IndexError,
        MdbError,
        MutableValueNotAllowed,
        WriteNotAllowed
    } errorCode;

    explicit OocError(const ErrorCode errorCode) : errorCode(errorCode) { }

    virtual void pythonize() const;
};

struct UnknownTypeError : OocError {
    PyObject* const type;

    explicit UnknownTypeError(PyObject* const type) :
        OocError(OocError::UnknownType),
        type(type)
    { }

    virtual void pythonize() const;
};

struct MdbError : OocError {
    const int mdbErrorCode;

    explicit MdbError(const int errorCode) :
        OocError(OocError::MdbError),
        mdbErrorCode(errorCode)
    { }

    virtual void pythonize() const;
};

#endif //OOCMAP_ERRORS_H
