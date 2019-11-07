package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

class CollectionRWP<M: BaseLMDBObject<M>, ItemType, CollectionType: Collection<ItemType>>(private val newCollectionInstance: () -> MutableCollection<ItemType>, private val itemCompanion: RWPCompanion<*, ItemType>, lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    VarSizeRWP<M, CollectionType>(lmdbObject, nullable) {
    @Suppress("UNCHECKED_CAST")
    override val readFn: (ByteBuffer, Int) -> CollectionType = { buffer, offset ->
        val numItems = buffer.readVarLong(offset)

        var off = offset + numItems.getVarLongSize()
        val ret = newCollectionInstance()
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

        ret as CollectionType
    }

    override val writeFn: (ByteBuffer, Int, CollectionType) -> Any? = { byteBuffer: ByteBuffer, i: Int, f: CollectionType ->
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

    override val getItemOnlySize: (CollectionType) -> Int = { list ->
        list.size.toLong().getVarLongSize() + list.map(itemCompanion::compSizeFn).sum() + list.map(itemCompanion::compSizeFn).map(Int::toLong).map(Long::getVarLongSize).sum()
    }
}