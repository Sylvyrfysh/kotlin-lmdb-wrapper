package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

abstract class WriteTXScope<T>(tx: LMDBTransaction) : ReadTXScope<T>(tx) {
    init {
        require(!tx.isReadOnly) { "Need a write transaction for WriteTXScope!" }
    }

    fun commit() {
        check(!isClosed) { "Cannot explicitly finish twice!" }
        tx.commit()
        isClosed = true
    }

    @Throws(IllegalArgumentException::class)
    override fun close() {
        if (!isClosed) {
            isClosed = true
            tx.commit()
        }
    }
}