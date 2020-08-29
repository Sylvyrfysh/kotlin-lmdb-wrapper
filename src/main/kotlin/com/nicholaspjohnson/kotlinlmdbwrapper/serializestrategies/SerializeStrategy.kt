package com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies

import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.BigDecimalSerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.BigIntegerSerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.InstantSerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.UUIDSerializer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.overwriteWith
import kotlinx.serialization.modules.plus
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KClass

abstract class SerializeStrategy {
    internal abstract fun setNewSerializersModule(newSerializersModule: SerializersModule)

    abstract fun <T> serialize(serializer: KSerializer<T>, item: T): ByteArray
    abstract fun <T> deserialize(serializer: KSerializer<T>, item: ByteArray): T

    open fun <T> deserialize(serializer: KSerializer<T>, item: ByteBuffer): T {
        val data = ByteArray(item.remaining())
        item.get(data)
        return deserialize(serializer, data)
    }

    companion object {
        private var currentSerMod = SerializersModule {
            contextual(UUID::class, UUIDSerializer)
            contextual(BigInteger::class, BigIntegerSerializer)
            contextual(BigDecimal::class, BigDecimalSerializer)
            contextual(Instant::class, InstantSerializer)
        }
        private val serializers = ArrayList<SerializeStrategy>()

        fun registerSerializeStrategy(serializer: SerializeStrategy) {
            serializers += serializer
            serializer.setNewSerializersModule(currentSerMod)
        }

        fun <T : Any> addContextualSerializer(cls: KClass<T>, serializer: KSerializer<T>, overwrite: Boolean = false) {
            val addSM = SerializersModule {
                contextual(cls, serializer)
            }
            currentSerMod = if (overwrite) currentSerMod.overwriteWith(addSM) else currentSerMod.plus(addSM)
            serializers.forEach { it.setNewSerializersModule(currentSerMod) }
        }

        fun <Base : Any, Sub : Base> addPolymorphicSerializer(
            baseCls: KClass<Base>,
            actCls: KClass<Sub>,
            serializer: KSerializer<Sub>,
            overwrite: Boolean = false
        ) {
            val addSM = SerializersModule {
                polymorphic(baseCls, actCls, serializer)
            }
            currentSerMod = if (overwrite) currentSerMod.overwriteWith(addSM) else currentSerMod.plus(addSM)
            serializers.forEach { it.setNewSerializersModule(currentSerMod) }
        }
    }
}
