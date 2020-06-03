package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import org.lwjgl.system.MemoryStack

abstract class ReadTXScope<T>(val tx: LMDBTransaction) : AutoCloseable {
    internal var isClosed: Boolean = false

    abstract fun exec(stack: MemoryStack): T

    fun abort() {
        check(!isClosed) { "Cannot explicitly finish twice!" }
        tx.abort()
    }

    fun abortSilent() {
        if (!isClosed) {
            isClosed = true
            tx.abort()
        }
    }

    fun finish() {
        check(!isClosed) { "Cannot explicitly finish twice!" }
        close()
    }

    @Throws(IllegalArgumentException::class)
    override fun close() {
        if (!isClosed) {
            isClosed = true
            tx.abort()
        }
    }
}