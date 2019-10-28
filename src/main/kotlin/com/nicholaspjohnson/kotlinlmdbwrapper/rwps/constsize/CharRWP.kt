package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Char] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class CharRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Char?>(obj, nullable) {
    override val itemSize: Int = 2
    override val readFn: (ByteBuffer, Int) -> Char? = ByteBuffer::getChar
    override val writeFn: (ByteBuffer, Int, Char?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<CharRWP<*>, Char?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Char? = buffer.getChar(offset)

        override fun compSizeFn(item: Char?): Int = Char.SIZE_BYTES

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Char?) {
            buffer.putChar(offset, item!!)
        }
    }
}
