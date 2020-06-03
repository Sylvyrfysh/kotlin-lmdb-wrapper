package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import org.lwjgl.util.lmdb.LMDB
import java.io.Closeable

class LMDBTransaction(val isReadOnly: Boolean, val tx: Long) : Closeable {
    private var isClosed: Boolean = false

    fun commit() {
        check(!isClosed)
        LMDB_CHECK(LMDB.mdb_txn_commit(tx))
        isClosed = true
    }

    fun abort() {
        check(!isClosed)
        close()
    }

    override fun close() {
        if (!isClosed) {
            LMDB.mdb_txn_abort(tx)
            isClosed = true
        }
    }
}
