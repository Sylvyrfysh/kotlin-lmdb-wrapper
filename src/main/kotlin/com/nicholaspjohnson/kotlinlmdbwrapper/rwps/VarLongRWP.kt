package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.*
import java.nio.ByteBuffer

/**
 * A default [Long] RWP that will act on instances of the class [M].
 * Writes and reads data as a VarLong internally.
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [VarSizeRWP],
 */
class VarLongRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String) : VarSizeRWP<M, Long?>(obj, name) {
    override val readFn: (ByteBuffer, Int) -> Long = ByteBuffer::readVarLong
    override val writeFn: (ByteBuffer, Int, Long?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (Long?) -> Int = ::compSizeFn

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Reads and returns the non-null value in [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: Long?) {
            buffer.writeVarLong(offset, value!!)
        }

        /**
         * Returns the raw size of [item] when encoded as a varlong.
         */
        private fun compSizeFn(item: Long?): Int {
            return item!!.getVarLongSize()
        }
    }
}
