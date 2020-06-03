package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import java.nio.ByteBuffer

/**
 * A constant sized item of type [ItemType] contained in class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [AbstractRWP].
 */
abstract class ConstSizeRWP<M: LMDBObject<M>, ItemType>(lmdbObject: LMDBObject<M>, nullable: Boolean) :
    AbstractRWP<M, ItemType>(lmdbObject, nullable) {
    /**
     * The constant size of this item.
     */
    abstract val itemSize: Int
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    protected abstract val readFn: (ByteBuffer, Int) -> ItemType
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    protected abstract val writeFn: (ByteBuffer, Int, ItemType) -> Any?

    override val getSize: (ItemType) -> Int = { itemSize }

    override fun writeToDB(
        writeBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int {
        return write(writeBuffer, startingOffset, nullStoreOption) { off ->
            writeFn(writeBuffer, off, field!!)
            return@write itemSize
        }
    }

    override fun readFromDB(
        readBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int {
        return read(readBuffer, startingOffset, nullStoreOption) { off ->
            field = readFn(readBuffer, off)
            return@read itemSize
        }
    }
}