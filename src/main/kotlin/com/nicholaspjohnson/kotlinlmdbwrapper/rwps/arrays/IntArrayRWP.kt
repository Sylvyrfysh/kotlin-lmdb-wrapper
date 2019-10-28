package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class IntArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, IntArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> IntArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, IntArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (IntArray?) -> Int = ::compSizeFn

    companion object: RWPCompanion<IntArrayRWP<*>, IntArray?> {
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: IntArray?) {
            buffer.position(offset)
            buffer.asIntBuffer().put(item!!)
            buffer.position(0)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): IntArray {
            val ret = IntArray(buffer.remaining() / Int.SIZE_BYTES)
            buffer.asIntBuffer().get(ret)
            return ret
        }

        override fun compSizeFn(item: IntArray?): Int {
            return item!!.size * Int.SIZE_BYTES
        }
    }
}