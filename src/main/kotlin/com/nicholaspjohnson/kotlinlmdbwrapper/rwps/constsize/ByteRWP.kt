package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Byte] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [ConstSizeRWP]
 */
class ByteRWP<M: LMDBObject<M>>(obj: LMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Byte?>(obj, nullable) {
    override val itemSize: Int = 1
    override val readFn: (ByteBuffer, Int) -> Byte? = ByteBuffer::get
    override val writeFn: (ByteBuffer, Int, Byte?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<ByteRWP<*>, Byte?> {
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Byte? = buffer.get(offset)

        override fun compSizeFn(item: Byte?): Int = 1

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Byte?) {
            buffer.put(offset, item!!)
        }
    }
}
