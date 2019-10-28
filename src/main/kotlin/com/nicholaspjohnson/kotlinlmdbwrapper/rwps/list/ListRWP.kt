package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

class ListRWP<M: BaseLMDBObject<M>, R, F: List<R>>(private val newListInstance: () -> MutableList<R>, private val itemCompanion: RWPCompanion<*, R>, lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    VarSizeRWP<M, F>(lmdbObject, nullable) {
    @Suppress("UNCHECKED_CAST")
    override val readFn: (ByteBuffer, Int) -> F = { buffer, offset ->
        val numItems = buffer.readVarLong(offset)

        var off = offset + numItems.getVarLongSize()
        val ret = newListInstance()
        repeat(numItems.toInt()) {
            val len = buffer.readVarLong(off)
            off += len.getVarLongSize()
            buffer.position(off)
            buffer.limit(off + len.toInt())
            val readItem = itemCompanion.compReadFn(buffer, off)
            buffer.limit(buffer.capacity())
            off += len.toInt()
            ret.add(readItem)
        }
        buffer.position(0)

        ret as F
    }

    override val writeFn: (ByteBuffer, Int, F) -> Any? = { byteBuffer: ByteBuffer, i: Int, f: F ->
        val items = field!!.size.toLong()
        val itemsLen = items.getVarLongSize()
        byteBuffer.writeVarLong(i, items)

        var off = i + itemsLen
        field!!.forEach {
            val len = itemCompanion.compSizeFn(it)
            byteBuffer.writeVarLong(off, len.toLong())
            off += len.toLong().getVarLongSize()
            itemCompanion.compWriteFn(byteBuffer, off, it)
            off += len
        }
    }

    override val getItemOnlySize: (F) -> Int = { list ->
        list.size.toLong().getVarLongSize() + list.map(itemCompanion::compSizeFn).sum() + list.map(itemCompanion::compSizeFn).map(Int::toLong).map(Long::getVarLongSize).sum()
    }
}