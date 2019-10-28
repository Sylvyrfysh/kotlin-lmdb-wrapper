package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Int] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [ConstSizeRWP]
 */
class IntRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Int?>(obj, nullable) {
    override val itemSize: Int = Int.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> Int = ByteBuffer::getInt
    override val writeFn: (ByteBuffer, Int, Int?) -> Any? =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<IntRWP<*>, Int?> {
        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Int?) {
            buffer.putInt(offset, item!!)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): Int? = buffer.getInt(offset)

        override fun compSizeFn(item: Int?): Int = Int.SIZE_BYTES
    }
}
