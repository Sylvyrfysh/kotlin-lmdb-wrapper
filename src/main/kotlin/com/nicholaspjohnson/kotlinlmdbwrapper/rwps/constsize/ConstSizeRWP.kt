package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import java.nio.ByteBuffer

/**
 * A constant sized item of type [R] contained in class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [AbstractRWP].
 */
abstract class ConstSizeRWP<M: BaseLMDBObject<M>, R>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    AbstractRWP<M, R>(lmdbObject, nullable) {
    /**
     * The constant size of this item.
     */
    internal abstract val itemSize: Int
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    protected abstract val readFn: (ByteBuffer, Int) -> R
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    protected abstract val writeFn: (ByteBuffer, Int, R) -> Any?

    override val getSize: (R) -> Int = { itemSize }

    override fun writeToDB(writeBuffer: ByteBuffer, startingOffset: Int): Int {
        return write(writeBuffer, startingOffset) { off ->
            writeFn(writeBuffer, off, field!!)
            return@write itemSize
        }
    }

    override fun readFromDB(readBuffer: ByteBuffer, startingOffset: Int): Int {
        return read(readBuffer, startingOffset) { off ->
            field = readFn(readBuffer, off)
            return@read itemSize
        }
    }
}