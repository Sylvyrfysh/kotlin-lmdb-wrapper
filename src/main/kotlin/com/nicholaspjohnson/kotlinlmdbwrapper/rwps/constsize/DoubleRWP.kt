package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Double] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class DoubleRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Double?>(obj, name) {
    override val itemSize: Int = java.lang.Double.BYTES
    override val readFn: (ByteBuffer, Int) -> Double? = ByteBuffer::getDouble
    override val writeFn: (ByteBuffer, Int, Double?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Double?) {
            buffer.putDouble(offset, value!!)
        }
    }
}
