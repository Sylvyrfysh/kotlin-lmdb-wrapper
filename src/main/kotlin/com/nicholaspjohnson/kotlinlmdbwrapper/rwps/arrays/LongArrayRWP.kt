package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class LongArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, propertyName: String): VarSizeRWP<M, LongArray?>(lmdbObject, propertyName) {
    override val readFn: (ByteBuffer, Int) -> LongArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, LongArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (LongArray?) -> Int = ::compSizeFn

    companion object {
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, item: LongArray?) {
            buffer.position(offset)
            buffer.asLongBuffer().put(item!!)
            buffer.position(0)
        }

        private fun compReadFn(buffer: ByteBuffer, offset: Int): LongArray {
            val ret = LongArray(buffer.remaining() / Long.SIZE_BYTES)
            buffer.asLongBuffer().get(ret)
            return ret
        }

        private fun compSizeFn(item: LongArray?): Int {
            return item!!.size * Long.SIZE_BYTES
        }
    }
}