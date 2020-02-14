package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [DoubleArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class DoubleArrayRWP<M: LMDBObject<M>>(lmdbObject: LMDBObject<M>, nullable: Boolean): VarSizeRWP<M, DoubleArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> DoubleArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, DoubleArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (DoubleArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<DoubleArrayRWP<*>, DoubleArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: DoubleArray?) {
            buffer.position(offset)
            buffer.asDoubleBuffer().put(item!!)
            buffer.position(0)
        }

        /**
         * Reads and returns the [DoubleArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): DoubleArray {
            val ret = DoubleArray(buffer.remaining() / java.lang.Double.BYTES)
            buffer.asDoubleBuffer().get(ret)
            return ret
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: DoubleArray?): Int {
            return item!!.size * java.lang.Double.BYTES
        }
    }
}