package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Boolean] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class BoolRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, propertyName: String) : ConstSizeRWP<M, Boolean?>(lmdbObject, propertyName) {
    override val itemSize: Int = 1
    override val readFn: (ByteBuffer, Int) -> Boolean = ::compReadFn
    override val writeFn: (ByteBuffer, Int, Boolean?) -> Unit = ::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Boolean?) {
            buffer.put(offset, if (value!!) 1.toByte() else 0.toByte())
        }

        /**
         * Reads and returns the non-null value from [buffer] at [offset].
         */
        private fun compReadFn(buffer: ByteBuffer, offset: Int): Boolean {
            return buffer.get(offset) != 0.toByte()
        }
    }
}
