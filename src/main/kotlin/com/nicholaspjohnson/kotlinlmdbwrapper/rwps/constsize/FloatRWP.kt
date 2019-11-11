package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Float] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [ConstSizeRWP]
 */
class FloatRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Float?>(obj, nullable) {
    override val itemSize: Int = java.lang.Float.BYTES
    override val readFn: (ByteBuffer, Int) -> Float? = ByteBuffer::getFloat
    override val writeFn: (ByteBuffer, Int, Float?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<FloatRWP<*>, Float?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Float? = buffer.getFloat(offset)

        override fun compSizeFn(item: Float?): Int = java.lang.Float.BYTES

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Float?) {
            buffer.putFloat(offset, item!!)
        }
    }
}
