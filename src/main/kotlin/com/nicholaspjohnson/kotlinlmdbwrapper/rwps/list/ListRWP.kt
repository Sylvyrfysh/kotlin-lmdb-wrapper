package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

class ListRWP<M: BaseLMDBObject<M>, ItemType, ListType: List<ItemType>>(private val newListInstance: () -> MutableList<ItemType>, private val itemCompanion: RWPCompanion<*, ItemType>, lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    VarSizeRWP<M, ListType>(lmdbObject, nullable) {
    @Suppress("UNCHECKED_CAST")
    override val readFn: (ByteBuffer, Int) -> ListType = { buffer, offset ->
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

        ret as ListType
    }

    override val writeFn: (ByteBuffer, Int, ListType) -> Any? = { byteBuffer: ByteBuffer, i: Int, f: ListType ->
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

    override val getItemOnlySize: (ListType) -> Int = { list ->
        list.size.toLong().getVarLongSize() + list.map(itemCompanion::compSizeFn).sum() + list.map(itemCompanion::compSizeFn).map(Int::toLong).map(Long::getVarLongSize).sum()
    }
}