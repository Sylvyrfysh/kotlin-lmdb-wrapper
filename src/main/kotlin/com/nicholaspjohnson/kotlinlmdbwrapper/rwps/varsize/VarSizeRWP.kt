package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

/**
 * A variably sized item of type [R] contained in class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [AbstractRWP].
 */
abstract class VarSizeRWP<M: BaseLMDBObject<M>, R>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    AbstractRWP<M, R>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    protected abstract val readFn: (ByteBuffer, Int) -> R
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    protected abstract val writeFn: (ByteBuffer, Int, R) -> Any?
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    protected abstract val getItemOnlySize: (R) -> Int

    override val getSize: (R) -> Int = { value ->
        val sz = getItemOnlySize(value!!)
        sz + sz.toLong().getVarLongSize()
    }

    override fun writeToDB(writeBuffer: ByteBuffer, startingOffset: Int): Int {
        return write(writeBuffer, startingOffset) { off ->
            val diskSize = getItemOnlySize(field!!).toLong()
            writeBuffer.writeVarLong(off, diskSize)
            writeFn(writeBuffer, off + diskSize.getVarLongSize(), field!!)
            return@write getSize(field!!)
        }
    }

    override fun readFromDB(readBuffer: ByteBuffer, startingOffset: Int): Int {
        return read(readBuffer, startingOffset) { off ->
            val diskSize = readBuffer.readVarLong(off)
            readBuffer.limit((off + diskSize.getVarLongSize() + diskSize).toInt())
            readBuffer.position(off + diskSize.getVarLongSize())
            field = readFn(readBuffer, off + diskSize.getVarLongSize())
            readBuffer.limit(readBuffer.capacity())
            readBuffer.position(0)
            return@read (diskSize.getVarLongSize() + diskSize).toInt()
        }
    }
}