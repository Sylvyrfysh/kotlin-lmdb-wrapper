package com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies

import com.nicholaspjohnson.kotlinlmdbwrapper.LongKeyMarkerSerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.UUIDKeyMarkerSerializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.protobuf.ProtoBuf
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

class ProtoBufSerializeStrategy(originalProtoBuf: ProtoBuf = DEFAULT_PROTO_BUF): SerializeStrategy() {
    private var pbuf = ProtoBuf(originalProtoBuf) {
        encodeDefaults = false
    }

    override fun setNewSerializersModule(newSerializersModule: SerializersModule) {
        pbuf = ProtoBuf(pbuf) {
            serializersModule = newSerializersModule
        }
    }

    private val longBuffer = ByteBuffer.allocate(Long.SIZE_BYTES).order(ByteOrder.nativeOrder())
    private val uuidBuffer = ByteBuffer.allocate(Long.SIZE_BYTES * 2).order(ByteOrder.nativeOrder())

    override fun <T> serialize(serializer: KSerializer<T>, item: T): ByteArray {
        if (serializer === LongKeyMarkerSerializer) {
            longBuffer.putLong(0, item as Long)
            return longBuffer.array()
        } else if (serializer === UUIDKeyMarkerSerializer) {
            uuidBuffer.putLong(8, (item as UUID).mostSignificantBits)
            uuidBuffer.putLong(0, item.leastSignificantBits)
            return uuidBuffer.array()
        }
        return pbuf.encodeToByteArray(serializer, item)
    }

    override fun <T> deserialize(serializer: KSerializer<T>, item: ByteArray): T {
        if (serializer === LongKeyMarkerSerializer) {
            longBuffer.position(0)
            longBuffer.put(item, 0, 8)
            return longBuffer.getLong(0) as T
        } else if (serializer === UUIDKeyMarkerSerializer) {
            uuidBuffer.position(0)
            uuidBuffer.put(item, 0, 16)
            return UUID(uuidBuffer.getLong(8), uuidBuffer.getLong(0)) as T
        }
        return pbuf.decodeFromByteArray(serializer, item)
    }

    companion object {
        private val DEFAULT_PROTO_BUF = ProtoBuf { encodeDefaults = false }
        val DEFAULT = ProtoBufSerializeStrategy(DEFAULT_PROTO_BUF)

        init {
            registerSerializeStrategy(DEFAULT)
        }
    }
}