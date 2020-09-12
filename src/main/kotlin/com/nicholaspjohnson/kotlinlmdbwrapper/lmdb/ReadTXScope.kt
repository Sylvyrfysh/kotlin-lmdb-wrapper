package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

/**
 * A DSL class for use inside read transaction blocks. Uses the given [tx] to interface with the environment.
 */
open class ReadTXScope(val tx: LMDBTransaction) : AutoCloseable {
    fun abort() {
        check(!tx.isClosed) { "Cannot explicitly finish twice!" }
        tx.abort()
    }

    /**
     *
     */
    @PublishedApi
    internal fun abortSilent() {
        if (!tx.isClosed) {
            tx.abort()
        }
    }

    fun finish() {
        check(!tx.isClosed) { "Cannot explicitly finish twice!" }
        close()
    }

    @Throws(IllegalArgumentException::class)
    override fun close() {
        if (!tx.isClosed) {
            tx.abort()
        }
    }
}