package com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies

import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.overwriteWith
import kotlinx.serialization.modules.plus
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.collections.ArrayList
import kotlin.reflect.KClass

/**
 * A way to serialize database items.
 */
abstract class SerializeStrategy {
    /**
     * Should recreate the underlying serializer with the [newSerializersModule] that is passed.
     */
    protected abstract fun setNewSerializersModule(newSerializersModule: SerializersModule)

    /**
     * Uses the given [serializer] to serialize the [item] of type [T] into a [ByteArray].
     */
    abstract fun <T> serialize(serializer: KSerializer<T>, item: T): ByteArray

    /**
     * Uses the given [serializer] to deserialize the [item] into a [T].
     */
    abstract fun <T> deserialize(serializer: KSerializer<T>, item: ByteArray): T

    /**
     * Convenience function to deserialize the [item] with the [serializer] into a [T] directly from a [ByteBuffer]. If
     * the deserializer can work directly with a [ByteBuffer], this method can be overridden to achieve faster
     * deserialization in some cases.
     */
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
            contextual(Duration::class, DurationSerializer)
        }
        private val serializers = ArrayList<SerializeStrategy>()

        /**
         * This will register [serializer] to use the default serializers as well as any globally registered
         * serializers.
         */
        fun registerSerializeStrategy(serializer: SerializeStrategy) {
            serializers += serializer
            serializer.setNewSerializersModule(currentSerMod)
        }

        /**
         * Adds a contextual serializer for all globally registered serializers for the specified [cls] of type [T],
         * using the [serializer]. The [overwrite] flag specifies if this should become the new global serializer for
         * the type [T].
         */
        fun <T : Any> addContextualSerializer(cls: KClass<T>, serializer: KSerializer<T>, overwrite: Boolean = false) {
            val addSM = SerializersModule {
                contextual(cls, serializer)
            }
            currentSerMod = if (overwrite) currentSerMod.overwriteWith(addSM) else currentSerMod.plus(addSM)
            serializers.forEach { it.setNewSerializersModule(currentSerMod) }
        }

        /**
         * Adds a polymorphic serializer for all globally registered serializers for the specified [actCls] of type
         * [Sub], using the [serializer]. The [overwrite] flag specifies if this should become the new global serializer
         * for the type [Sub].
         */
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
