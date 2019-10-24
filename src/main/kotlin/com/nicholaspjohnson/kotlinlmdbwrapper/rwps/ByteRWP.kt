package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Byte] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class ByteRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Byte?>(obj, name) {
    override val itemSize: Int = 1
    override val readFn: (ByteBuffer, Int) -> Byte? = ByteBuffer::get
    override val writeFn: (ByteBuffer, Int, Byte?) -> Unit = ::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Byte?) {
            buffer.put(offset, value!!)
        }
    }
}
