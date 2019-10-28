package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

class BoolArrayRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, BooleanArray?>(lmdbObject, nullable) {
    override val readFn: (ByteBuffer, Int) -> BooleanArray = ::compReadFn
    override val writeFn: (ByteBuffer, Int, BooleanArray?) -> Any? = ::compWriteFn
    override val getItemOnlySize: (BooleanArray?) -> Int = ::compSizeFn

    companion object: RWPCompanion<BoolArrayRWP<*>, BooleanArray?> {
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: BooleanArray?) {
            buffer.position(offset)
            buffer.put(item!!.map { if(it) 1.toByte() else 0.toByte() }.toByteArray())
            buffer.position(0)
        }

        override fun compReadFn(buffer: ByteBuffer, offset: Int): BooleanArray {
            val ret = ByteArray(buffer.remaining())
            buffer.get(ret)
            return ret.map { it != 0.toByte() }.toBooleanArray()
        }

        override fun compSizeFn(item: BooleanArray?): Int {
            return item!!.size
        }
    }
}