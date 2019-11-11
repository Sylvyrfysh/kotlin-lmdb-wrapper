package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [ShortArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class ShortArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, ShortArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> ShortArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, ShortArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (ShortArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<ShortArrayRWP<*>, ShortArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: ShortArray?) {
            buffer.position(offset)
            buffer.asShortBuffer().put(item!!)
            buffer.position(0)
        }

        /**
         * Reads and returns the [ShortArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): ShortArray {
            val ret = ShortArray(buffer.remaining() / Short.SIZE_BYTES)
            buffer.asShortBuffer().get(ret)
            return ret
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: ShortArray?): Int {
            return item!!.size * Short.SIZE_BYTES
        }
    }
}