package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [BooleanArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class BoolArrayRWP<M: LMDBObject<M>>(lmdbObject: LMDBObject<M>, nullable: Boolean): VarSizeRWP<M, BooleanArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> BooleanArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, BooleanArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (BooleanArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<BoolArrayRWP<*>, BooleanArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: BooleanArray?) {
            buffer.position(offset)
            buffer.put(item!!.map { if(it) 1.toByte() else 0.toByte() }.toByteArray())
            buffer.position(0)
        }

        /**
         * Reads and returns the [BooleanArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): BooleanArray {
            val ret = ByteArray(buffer.remaining())
            buffer.get(ret)
            return ret.map { it != 0.toByte() }.toBooleanArray()
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: BooleanArray?): Int {
            return item!!.size
        }
    }
}