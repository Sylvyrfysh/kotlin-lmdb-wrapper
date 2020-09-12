package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

/**
 * A DSL class for use inside write transaction blocks. Uses the given [tx] to interface with the environment.
 */
class WriteTXScope(tx: LMDBTransaction) : ReadTXScope(tx) {
    init {
        require(!tx.isReadOnly) { "Need a write transaction for WriteTXScope!" }
    }

    fun commit() {
        check(!tx.isClosed) { "Cannot explicitly finish twice!" }
        tx.commit()
    }

    @Throws(IllegalArgumentException::class)
    override fun close() {
        if (!tx.isClosed) {
            tx.commit()
        }
    }
}