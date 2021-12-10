package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.internal

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.DataNotFoundException
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBException
import org.lwjgl.util.lmdb.LMDB.*

/**
 * Checks that [rc] is a success code, otherwise throws a [LMDBException]
 */
@Throws(LMDBException::class)
fun LMDB_CHECK(rc: Int) {
    if (rc == MDB_NOTFOUND) throw DataNotFoundException("The key supplied does not have any data in the DB!")
    if (rc != MDB_SUCCESS) throw LMDBException(rc, mdb_strerror(rc))
}