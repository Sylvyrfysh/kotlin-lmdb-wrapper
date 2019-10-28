package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class ShortArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, propertyName: String): VarSizeRWP<M, ShortArray?>(lmdbObject, propertyName) {
    override val readFn: (ByteBuffer, Int) -> ShortArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, ShortArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (ShortArray?) -> Int = ::compSizeFn

    companion object {
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, item: ShortArray?) {
            buffer.position(offset)
            buffer.asShortBuffer().put(item!!)
            buffer.position(0)
        }

        private fun compReadFn(buffer: ByteBuffer, offset: Int): ShortArray {
            val ret = ShortArray(buffer.remaining() / Short.SIZE_BYTES)
            buffer.asShortBuffer().get(ret)
            return ret
        }

        private fun compSizeFn(item: ShortArray?): Int {
            return item!!.size * Short.SIZE_BYTES
        }
    }
}