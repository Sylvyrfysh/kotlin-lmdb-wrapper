package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.internal.LMDB_CHECK
import org.lwjgl.util.lmdb.LMDB
import java.io.Closeable

/**
 * Class wrapping a [tx] that [isReadOnly].
 */
class LMDBTransaction(val isReadOnly: Boolean, val tx: Long) : Closeable {
    internal var isClosed: Boolean = false

    /**
     * Commits the transaction, or throws an [IllegalStateException] if an abort or commit has already been performed.
     */
    fun commit() {
        check(!isClosed)
        LMDB_CHECK(LMDB.mdb_txn_commit(tx))
        isClosed = true
    }

    /**
     * Aborts the transaction, or throws an [IllegalStateException] if an abort or commit has already been performed.
     */
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
