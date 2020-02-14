package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Boolean] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [ConstSizeRWP]
 */
class BoolRWP<M: LMDBObject<M>>(lmdbObject: LMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, Boolean?>(lmdbObject, nullable) {
    override val itemSize: Int = 1
    override val readFn: (ByteBuffer, Int) -> Boolean =
        Companion::compReadFn
    override val writeFn: (ByteBuffer, Int, Boolean?) -> Unit =
        Companion::compWriteFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<BoolRWP<*>, Boolean?> {
        override fun compSizeFn(item: Boolean?): Int = 1

        /**
         * Writes the non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Boolean?) {
            buffer.put(offset, if (item!!) 1.toByte() else 0.toByte())
        }

        /**
         * Reads and returns the non-null value from [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Boolean {
            return buffer.get(offset) != 0.toByte()
        }
    }
}
