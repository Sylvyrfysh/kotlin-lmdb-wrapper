package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class ByteArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, ByteArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> ByteArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, ByteArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (ByteArray?) -> Int = ::compSizeFn

    companion object {
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, item: ByteArray?) {
            buffer.position(offset)
            buffer.put(item!!)
            buffer.position(0)
        }

        private fun compReadFn(buffer: ByteBuffer, offset: Int): ByteArray {
            val ret = ByteArray(buffer.remaining())
            buffer.get(ret)
            return ret
        }

        private fun compSizeFn(item: ByteArray?): Int {
            return item!!.size * Byte.SIZE_BYTES
        }
    }
}