package com.nicholaspjohnson.kotlinlmdbwrapper

import org.lwjgl.util.lmdb.LMDB.MDB_SUCCESS
import org.lwjgl.util.lmdb.LMDB.mdb_strerror

/**
 * Checks that [rc] is a success code, otherwise throws a [IllegalArgumentException]
 */
@Throws(IllegalArgumentException::class)
fun LMDB_CHECK(rc: Int) {
    check(rc == MDB_SUCCESS) { mdb_strerror(rc) }
}