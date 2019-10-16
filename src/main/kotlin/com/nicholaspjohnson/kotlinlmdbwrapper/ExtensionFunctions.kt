package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer
import org.lwjgl.util.lmdb.LMDB.mdb_strerror
import org.lwjgl.util.lmdb.LMDB.MDB_SUCCESS
import kotlin.experimental.and

/**
 * Reads and returns a [Long] that has been stored as a VarLong in this buffer starting at [startPoint].
 */
fun ByteBuffer.readVarLong(startPoint: Int): Long {
    var idx = 0
    var ret = 0L
    do {
        ret += (this[startPoint + idx].toLong() and 0x7F) shl (idx * 7)
    } while (this[startPoint + idx++].toInt() and 0x80 == 0x80)
    return ret
}

/**
 * Writes the [Long] value [data] to this buffer starting at [startPoint].
 */
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

/**
 * Returns the number of bytes long this [Long] is as a VarLong.
 */
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

/**
 * Checks that [rc] is a success code, otherwise throws a [IllegalArgumentException]
 */
@Throws(IllegalArgumentException::class)
fun LMDB_CHECK(rc: Int) {
    check(rc == MDB_SUCCESS) { mdb_strerror(rc) }
}