package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Long] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class LongRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Long?>(obj, name) {
    override val itemSize: Int = Long.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Long? = ByteBuffer::getLong
    override val writeFn: (ByteBuffer, Int, Long?) -> Unit = ::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Long?) {
            buffer.putLong(offset, value!!)
        }
    }
}
