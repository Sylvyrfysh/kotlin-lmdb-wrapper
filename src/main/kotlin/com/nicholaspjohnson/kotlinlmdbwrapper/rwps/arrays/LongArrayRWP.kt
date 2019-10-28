package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class LongArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, LongArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> LongArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, LongArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (LongArray?) -> Int = ::compSizeFn

    companion object: RWPCompanion<LongArrayRWP<*>, LongArray?> {
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: LongArray?) {
            buffer.position(offset)
            buffer.asLongBuffer().put(item!!)
            buffer.position(0)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): LongArray {
            val ret = LongArray(buffer.remaining() / Long.SIZE_BYTES)
            buffer.asLongBuffer().get(ret)
            return ret
        }

        override fun compSizeFn(item: LongArray?): Int {
            return item!!.size * Long.SIZE_BYTES
        }
    }
}