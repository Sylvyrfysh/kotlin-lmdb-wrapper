package com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlin.text.Charsets.UTF_8

class JsonSerializeStrategy(originalJson: Json = DEFAULT_JSON) : SerializeStrategy() {
    private var json = Json(originalJson) { }

    override fun setNewSerializersModule(newSerializersModule: SerializersModule) {
        json = Json(json) {
            serializersModule = newSerializersModule
        }
    }

    override fun <T> serialize(serializer: KSerializer<T>, item: T): ByteArray {
        return json.encodeToString(serializer, item).toByteArray(UTF_8)
    }

    override fun <T> deserialize(serializer: KSerializer<T>, item: ByteArray): T {
        return json.decodeFromString(serializer, item.toString(UTF_8))
    }

    companion object {
        private val DEFAULT_JSON = Json {
            allowSpecialFloatingPointValues = true
            allowStructuredMapKeys = true
            encodeDefaults = false
        }
        val DEFAULT = JsonSerializeStrategy(DEFAULT_JSON)

        init {
            registerSerializeStrategy(DEFAULT)
        }
    }
}