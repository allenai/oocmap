#include "db.h"

#include "spooky.h"
#include "errors.h"

class GilUnlocker {
    PyThreadState* const m_threadState;

public:
    GilUnlocker() : m_threadState(PyGILState_Check() ? PyEval_SaveThread() : nullptr) { }
    ~GilUnlocker() {
        if(m_threadState != nullptr)
            PyEval_RestoreThread(m_threadState);
    }
};

MDB_txn* txn_begin(MDB_env* const mdb, const bool write) {
    GilUnlocker gil;

    const unsigned int flags = write ? 0 : MDB_RDONLY;
    MDB_txn* txn = nullptr;
    int mapsizePatience = 10;
    while(true) {
        int error = mdb_txn_begin(mdb, nullptr, flags, &txn);
        switch(error) {
        case 0:
            return txn;
        case MDB_MAP_RESIZED:
            if (mapsizePatience > 0) {
                mapsizePatience -= 1;
                error = mdb_env_set_mapsize(mdb, 0);
                if(error != 0)
                    throw MdbError(error);
                MDB_envinfo info;
                mdb_env_info(mdb, &info);
                continue;
            } else {
                throw MdbError(error);
            }
        default:
            throw MdbError(error);
        }
    }
}

void txn_commit(MDB_txn* const txn) {
    GilUnlocker gil;
    const int error = mdb_txn_commit(txn);
    if(error != 0)
        throw MdbError(error);
}

void txn_abort(MDB_txn* const txn) {
    GilUnlocker gil;
    mdb_txn_abort(txn); // This doesn't return any errors.
}

void open_db(MDB_txn* const txn, const char* const name, unsigned int flags, MDB_dbi* const dbi) {
    GilUnlocker gil;
    const int error = mdb_dbi_open(txn, name, flags | MDB_CREATE, dbi);
    if(error != 0)
        throw MdbError(error);
}

void put(
    MDB_txn* const txn,
    const MDB_dbi dbi,
    MDB_val* const key,
    MDB_val* const value,
    unsigned int flags
) {
    GilUnlocker gil;
    const int error = mdb_put(txn, dbi, key, value, flags);
    if(error != 0)
        throw MdbError(error);
}

void get(
    MDB_txn* const txn,
    const MDB_dbi dbi,
    MDB_val* const key,
    MDB_val* const value
) {
    GilUnlocker gil;
    const int error = mdb_get(txn, dbi, key, value);
    if(error != 0)
        throw MdbError(error);
}

uint64_t putImmutable(
    MDB_txn* const txn,
    const MDB_dbi dbi,
    MDB_val* const mdbVal,
    const unsigned char typeCode,
    const bool readonly
) {
    GilUnlocker gil;
    uint64_t key = SpookyHash::hash64(
        mdbVal->mv_data,
        mdbVal->mv_size,
        typeCode);
    MDB_val mdbKey = { .mv_size = sizeof(key), .mv_data = &key };

    if(readonly) {
        // In a readonly transaction, we turn the put() into a get() to check whether
        // the value is there.
        const int error = mdb_get(txn, dbi, &mdbKey, mdbVal);
        switch(error) {
        case 0:
            // We could check here for hash collisions, but we won't.
            break;
        case MDB_NOTFOUND:
            throw OocError(OocError::ImmutableValueNotFound);
        default:
            throw MdbError(error);
        }
    } else {
        put(txn, dbi, &mdbKey, mdbVal);
    }

    return key;
}

void del(MDB_txn* const txn, MDB_dbi dbi, MDB_val* key) {
    GilUnlocker gil;
    const int error = mdb_del(txn, dbi, key, nullptr);
    if(error != 0)
        throw MdbError(error);
}

MDB_cursor* cursor_open(MDB_txn* const txn, const MDB_dbi dbi) {
    MDB_cursor* result;
    const int error = mdb_cursor_open(txn, dbi, &result);
    if(error != 0)
        throw MdbError(error);
    return result;
}

void cursor_close(MDB_cursor* const cursor) {
    mdb_cursor_close(cursor);
    // Apparently this never fails?
}

void cursor_get(
    MDB_cursor* const cursor,
    MDB_val* const key,
    MDB_val* const data,
    const MDB_cursor_op op
) {
    GilUnlocker gil;
    const int error = mdb_cursor_get(cursor, key, data, op);
    if(error != 0)
        throw MdbError(error);
}
