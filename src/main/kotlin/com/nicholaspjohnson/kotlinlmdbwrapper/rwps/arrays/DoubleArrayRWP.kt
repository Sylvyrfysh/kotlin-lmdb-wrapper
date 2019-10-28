package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class DoubleArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, DoubleArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> DoubleArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, DoubleArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (DoubleArray?) -> Int = ::compSizeFn

    companion object: RWPCompanion<DoubleArrayRWP<*>, DoubleArray?> {
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: DoubleArray?) {
            buffer.position(offset)
            buffer.asDoubleBuffer().put(item!!)
            buffer.position(0)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): DoubleArray {
            val ret = DoubleArray(buffer.remaining() / java.lang.Double.BYTES)
            buffer.asDoubleBuffer().get(ret)
            return ret
        }

        override fun compSizeFn(item: DoubleArray?): Int {
            return item!!.size * java.lang.Double.BYTES
        }
    }
}