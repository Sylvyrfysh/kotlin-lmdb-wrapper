package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer

/**
 * A default [Char] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class CharRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : ConstSizeRWP<M, Char?>(obj, name) {
    override val itemSize: Int = 2
    override val readFn: (ByteBuffer, Int) -> Char? = ByteBuffer::getChar
    override val writeFn: (ByteBuffer, Int, Char?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Writes the non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Char?) {
            buffer.putChar(offset, value!!)
        }
    }
}
