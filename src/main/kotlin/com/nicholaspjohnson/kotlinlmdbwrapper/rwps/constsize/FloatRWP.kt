package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Float] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class FloatRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Float?>(obj, name) {
    override val itemSize: Int = java.lang.Float.BYTES
    override val readFn: (ByteBuffer, Int) -> Float? = ByteBuffer::getFloat
    override val writeFn: (ByteBuffer, Int, Float?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Float?) {
            buffer.putFloat(offset, value!!)
        }
    }
}
