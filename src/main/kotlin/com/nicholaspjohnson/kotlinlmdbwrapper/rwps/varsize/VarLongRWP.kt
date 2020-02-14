package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize

import com.nicholaspjohnson.kotlinlmdbwrapper.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import java.nio.ByteBuffer

/**
 * A default [Long] RWP that will act on instances of the class [M].
 * Writes and reads data as a VarLong internally.
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP],
 */
class VarLongRWP<M: LMDBObject<M>>(obj: LMDBObject<M>, nullable: Boolean) : VarSizeRWP<M, Long?>(obj, nullable) {
    override val readFn: (ByteBuffer, Int) -> Long = ByteBuffer::readVarLong
    override val writeFn: (ByteBuffer, Int, Long?) -> Any? =
        Companion::compWriteFn
    override val getItemOnlySize: (Long?) -> Int =
        Companion::compSizeFn

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<VarLongRWP<*>, Long?> {
        /**
        * Reads and returns the non-null value in [buffer] at [offset].
        */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): Long? = buffer.readVarLong(offset)

        /**
         * Writes non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: Long?) {
            buffer.writeVarLong(offset, item!!)
        }

        /**
         * Returns the raw size of [item] when encoded as a varlong.
         */
        override fun compSizeFn(item: Long?): Int {
            return item!!.getVarLongSize()
        }
    }
}
