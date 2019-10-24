package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Int] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class IntRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Int?>(obj, name) {
    override val itemSize: Int = Int.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Int = ByteBuffer::getInt
    override val writeFn: (ByteBuffer, Int, Int?) -> Any? = ::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Int?) {
            buffer.putInt(offset, value!!)
        }
    }
}
