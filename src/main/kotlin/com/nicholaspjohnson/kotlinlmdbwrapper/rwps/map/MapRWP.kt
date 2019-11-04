package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.map

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.getVarLongSize
import com.nicholaspjohnson.kotlinlmdbwrapper.readVarLong
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.writeVarLong
import java.nio.ByteBuffer

class MapRWP <M: BaseLMDBObject<M>, KeyType, DataType, MapType: Map<KeyType, DataType>> (
    private val newMapInstance: () -> MutableMap<KeyType, DataType>,
    private val keyCompanion: RWPCompanion<*, KeyType>,
    private val dataCompanion: RWPCompanion<*, DataType>,
    lmdbObject: BaseLMDBObject<M>,
    nullable: Boolean
) : VarSizeRWP<M, MapType>(lmdbObject, nullable) {
    @Suppress("UNCHECKED_CAST")
    override val readFn: (ByteBuffer, Int) -> MapType = { buffer, off ->
        var offset = off
        val items = buffer.readVarLong(offset)
        offset += items.getVarLongSize()

        val retMap = newMapInstance()

        repeat(items.toInt()) {
            val keyLen = buffer.readVarLong(offset)
            offset += keyLen.getVarLongSize()
            buffer.position(offset)
            buffer.limit(offset + keyLen.toInt())
            val key = keyCompanion.compReadFn(buffer, offset)
            buffer.limit(buffer.capacity())
            offset += keyLen.toInt()

            val dataLen = buffer.readVarLong(offset)
            offset += dataLen.getVarLongSize()
            buffer.position(offset)
            buffer.limit(offset + dataLen.toInt())
            val data = dataCompanion.compReadFn(buffer, offset)
            buffer.limit(buffer.capacity())
            offset += dataLen.toInt()

            retMap[key] = data
        }

        retMap as MapType
    }

    override val writeFn: (ByteBuffer, Int, MapType) -> Any? = { buffer, off, value ->
        var offset = off
        buffer.writeVarLong(offset, value.size.toLong())
        offset += value.size.toLong().getVarLongSize()
        value.forEach { (key, data) ->
            val keySize = keyCompanion.compSizeFn(key)
            buffer.writeVarLong(offset, keySize.toLong())
            offset += keySize.toLong().getVarLongSize()
            keyCompanion.compWriteFn(buffer, offset, key)
            offset += keySize

            val dataSize = dataCompanion.compSizeFn(data)
            buffer.writeVarLong(offset, dataSize.toLong())
            offset += dataSize.toLong().getVarLongSize()
            dataCompanion.compWriteFn(buffer, offset, data)
            offset += dataSize
        }
    }

    override val getItemOnlySize: (MapType) -> Int = { map ->
        map.size.toLong().getVarLongSize() + map.entries.map {
            val ks = keyCompanion.compSizeFn(it.key).run {
                this + toLong().getVarLongSize()
            }
            val ds = dataCompanion.compSizeFn(it.value).run {
                this + toLong().getVarLongSize()
            }
            ks + ds
        }.sum()
    }
}