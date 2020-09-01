package com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies

import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.protobuf.ProtoBuf

class ProtoBufSerializeStrategy(originalProtoBuf: ProtoBuf = DEFAULT_PROTO_BUF): SerializeStrategy() {
    private var pbuf = ProtoBuf(originalProtoBuf) { }

    override fun setNewSerializersModule(newSerializersModule: SerializersModule) {
        pbuf = ProtoBuf(pbuf) {
            serializersModule = newSerializersModule
        }
    }

    override fun <T> serialize(serializer: KSerializer<T>, item: T): ByteArray {
        return pbuf.encodeToByteArray(serializer, item)
    }

    override fun <T> deserialize(serializer: KSerializer<T>, item: ByteArray): T {
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