package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Short] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [ConstSizeRWP]
 */
class ShortRWP<M: LMDBObject<M>>(obj: LMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Short?>(obj, nullable) {
    override val itemSize: Int = Short.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Short? = ByteBuffer::getShort
    override val writeFn: (ByteBuffer, Int, Short?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<ShortRWP<*>, Short?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Short? = buffer.getShort(offset)

        override fun compSizeFn(item: Short?): Int = Short.SIZE_BYTES

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Short?) {
            buffer.putShort(offset, item!!)
        }
    }
}
