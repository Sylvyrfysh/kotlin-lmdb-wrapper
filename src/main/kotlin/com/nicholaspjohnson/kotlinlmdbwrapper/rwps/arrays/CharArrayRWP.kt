package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class CharArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, propertyName: String): VarSizeRWP<M, CharArray?>(lmdbObject, propertyName) {
    override val readFn: (ByteBuffer, Int) -> CharArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, CharArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (CharArray?) -> Int = ::compSizeFn

    companion object {
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, item: CharArray?) {
            buffer.position(offset)
            buffer.asCharBuffer().put(item!!)
            buffer.position(0)
        }

        private fun compReadFn(buffer: ByteBuffer, offset: Int): CharArray {
            val ret = CharArray(buffer.remaining() / Char.SIZE_BYTES)
            buffer.asCharBuffer().get(ret)
            return ret
        }

        private fun compSizeFn(item: CharArray?): Int {
            return item!!.size * Char.SIZE_BYTES
        }
    }
}