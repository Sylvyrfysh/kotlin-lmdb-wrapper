package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer
import org.lwjgl.util.lmdb.LMDB.mdb_strerror
import org.lwjgl.util.lmdb.LMDB.MDB_SUCCESS

fun ByteBuffer.readVarLong(startPoint: Int): Long {
    var idx = 0
    var ret = 0L
    do {
        ret += (this[startPoint + idx].toLong() and 0x7F) shl (idx * 7)
    } while (this[startPoint + idx++] < 128.toByte())
    return ret
}

fun ByteBuffer.writeVarLong(startPoint: Int, data: Long) {
    var idx = 0
    var rem = data
    do {
        val hasMore = (rem ushr 7) != 0L
        val dChunk = ((rem and 0x7F) + if (hasMore) 0x80 else 0).toByte()
        rem = rem ushr 7
        this.put(startPoint + idx++, dChunk)
    } while (hasMore)
}

fun Long.getVarLongSize(): Int {
    var idx = 0
    var rem = this
    do {
        val hasMore = (rem ushr 7) != 0L
        rem = rem ushr 7
        ++idx
    } while (hasMore)
    return idx
}

fun LMDB_CHECK(rc: Int) {
    check(rc == MDB_SUCCESS) { mdb_strerror(rc) }
}