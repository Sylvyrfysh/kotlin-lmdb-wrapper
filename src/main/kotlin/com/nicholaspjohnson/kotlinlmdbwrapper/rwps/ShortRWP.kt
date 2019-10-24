package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Short] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class ShortRWP<M : BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Short?>(obj, name) {
    override val itemSize: Int = Short.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Short? = ByteBuffer::getShort
    override val writeFn: (ByteBuffer, Int, Short?) -> Unit = ::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Short?) {
            buffer.putShort(offset, value!!)
        }
    }
}
