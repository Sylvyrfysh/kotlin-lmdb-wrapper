package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

/**
 * A default [Collection] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 * Holds [newCollectionInstance] and [itemCompanion] for reading and writing purposes.
 */
class CollectionRWP<M: LMDBObject<M>, ItemType, CollectionType: Collection<ItemType>>(private val newCollectionInstance: () -> MutableCollection<ItemType>, private val itemCompanion: RWPCompanion<*, ItemType>, lmdbObject: LMDBObject<M>, nullable: Boolean) :
    VarSizeRWP<M, CollectionType>(lmdbObject, nullable) {
    /**
     * A function that takes the read buffer and offset and returns the value at that point.
     */
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

    /**
     * A function that takes the write buffer and offset and writes the given value at that point.
     */
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

    /**
     * A function that returns the size of the object when ready to be written on disk.
     */
    override val getItemOnlySize: (CollectionType) -> Int = { list ->
        list.size.toLong().getVarLongSize() + list.map(itemCompanion::compSizeFn).sum() + list.map(itemCompanion::compSizeFn).map(Int::toLong).map(Long::getVarLongSize).sum()
    }
}