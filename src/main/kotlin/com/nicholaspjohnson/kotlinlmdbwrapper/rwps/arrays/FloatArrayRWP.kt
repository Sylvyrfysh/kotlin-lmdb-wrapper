package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class FloatArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, FloatArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> FloatArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, FloatArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (FloatArray?) -> Int = ::compSizeFn

    companion object: RWPCompanion<FloatArrayRWP<*>, FloatArray?> {
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: FloatArray?) {
            buffer.position(offset)
            buffer.asFloatBuffer().put(item!!)
            buffer.position(0)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): FloatArray {
            val ret = FloatArray(buffer.remaining() / java.lang.Float.BYTES)
            buffer.asFloatBuffer().get(ret)
            return ret
        }

        override fun compSizeFn(item: FloatArray?): Int {
            return item!!.size * java.lang.Float.BYTES
        }
    }
}