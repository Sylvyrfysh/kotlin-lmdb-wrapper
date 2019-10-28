package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Long] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class LongRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Long?>(obj, nullable) {
    override val itemSize: Int = Long.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Long? = ByteBuffer::getLong
    override val writeFn: (ByteBuffer, Int, Long?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<LongRWP<*>, Long?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Long? = buffer.getLong(offset)

        override fun compSizeFn(item: Long?): Int = Long.SIZE_BYTES

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Long?) {
            buffer.putLong(offset, item!!)
        }
    }
}
