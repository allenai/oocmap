#ifndef OOCMAP_DB_H
#define OOCMAP_DB_H

#include <cstdint>
#include "lmdb.h"

MDB_txn* txn_begin(MDB_env* mdb, bool write);
void txn_commit(MDB_txn* txn);
void txn_abort(MDB_txn* txn);
void open_db(MDB_txn* txn, const char* name, unsigned int flags, MDB_dbi* dbi);

void put(
    MDB_txn* txn,
    MDB_dbi dbi,
    MDB_val* key,
    MDB_val* value,
    unsigned int flags = 0
);

bool get(
    MDB_txn* txn,
    MDB_dbi dbi,
    MDB_val* key,
    MDB_val* value
);

uint64_t putImmutable(
    MDB_txn* txn,
    MDB_dbi dbi,
    MDB_val* mdbVal,
    unsigned char typeCode,
    bool readonly = false
);

void del(MDB_txn* txn, MDB_dbi dbi, MDB_val* key);

MDB_cursor* cursor_open(MDB_txn* txn, MDB_dbi dbi);
void cursor_close(MDB_cursor* cursor);
bool cursor_get(MDB_cursor* cursor, MDB_val* key, MDB_val* data, MDB_cursor_op op);
void cursor_put(MDB_cursor* cursor, MDB_val* key, MDB_val* data, unsigned int flags = 0);
void cursor_del(MDB_cursor* cursor, unsigned int flags = 0);

#endif //OOCMAP_DB_H
