package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [LongArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class LongArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, LongArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> LongArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, LongArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (LongArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<LongArrayRWP<*>, LongArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: LongArray?) {
            buffer.position(offset)
            buffer.asLongBuffer().put(item!!)
            buffer.position(0)
        }

        /**
         * Reads and returns the [LongArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): LongArray {
            val ret = LongArray(buffer.remaining() / Long.SIZE_BYTES)
            buffer.asLongBuffer().get(ret)
            return ret
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: LongArray?): Int {
            return item!!.size * Long.SIZE_BYTES
        }
    }
}