package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

/**
 * A default [CharArray] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class CharArrayRWP<M: LMDBObject<M>>(lmdbObject: LMDBObject<M>, nullable: Boolean): VarSizeRWP<M, CharArray?>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
    override val readFn: (ByteBuffer, Int) -> CharArray = ::compReadFn
    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
    override val writeFn: (ByteBuffer, Int, CharArray?) -> Any? = ::compWriteFn
    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (CharArray?) -> Int = ::compSizeFn

    /**
     * Helper methods to make this usable in collections and mops.
     */
    companion object: RWPCompanion<CharArrayRWP<*>, CharArray?> {
        /**
         * Writes [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: CharArray?) {
            buffer.position(offset)
            buffer.asCharBuffer().put(item!!)
            buffer.position(0)
        }

        /**
         * Reads and returns the [CharArray] in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): CharArray {
            val ret = CharArray(buffer.remaining() / Char.SIZE_BYTES)
            buffer.asCharBuffer().get(ret)
            return ret
        }

        /**
         * Returns the size that [item] will be on disk.
         */
        override fun compSizeFn(item: CharArray?): Int {
            return item!!.size * Char.SIZE_BYTES
        }
    }
}