package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Double] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [ConstSizeRWP]
 */
class DoubleRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Double?>(obj, nullable) {
    override val itemSize: Int = java.lang.Double.BYTES
    override val readFn: (ByteBuffer, Int) -> Double? = ByteBuffer::getDouble
    override val writeFn: (ByteBuffer, Int, Double?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<DoubleRWP<*>, Double?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Double? = buffer.getDouble(offset)

        override fun compSizeFn(item: Double?): Int = java.lang.Double.BYTES

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Double?) {
            buffer.putDouble(offset, item!!)
        }
    }
}
