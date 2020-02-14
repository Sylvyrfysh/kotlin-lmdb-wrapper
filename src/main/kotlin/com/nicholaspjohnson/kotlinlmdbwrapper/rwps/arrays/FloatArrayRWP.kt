package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [FloatArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class FloatArrayRWP<M: LMDBObject<M>>(lmdbObject: LMDBObject<M>, nullable: Boolean): VarSizeRWP<M, FloatArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> FloatArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, FloatArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (FloatArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<FloatArrayRWP<*>, FloatArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: FloatArray?) {
            buffer.position(offset)
            buffer.asFloatBuffer().put(item!!)
            buffer.position(0)
        }

        /**
         * Reads and returns the [FloatArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): FloatArray {
            val ret = FloatArray(buffer.remaining() / java.lang.Float.BYTES)
            buffer.asFloatBuffer().get(ret)
            return ret
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: FloatArray?): Int {
            return item!!.size * java.lang.Float.BYTES
        }
    }
}