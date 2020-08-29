package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ByteArraySerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.util.*

object UUIDSerializer: KSerializer<UUID> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("BinaryUUID") {
        element(MSB, Long.serializer().descriptor)
        element(LSB, Long.serializer().descriptor)
    }

    override fun deserialize(decoder: Decoder): UUID {
        val s = decoder.beginStructure(descriptor)
        var msb: Long? = null
        var lsb: Long? = null
        if (s.decodeSequentially()) {
            msb = s.decodeLongElement(descriptor, MSB_IDX)
            lsb = s.decodeLongElement(descriptor, LSB_IDX)
        } else loop@ while (true) {
            when (val i = s.decodeElementIndex(descriptor)) {
                MSB_IDX                      -> msb = s.decodeLongElement(descriptor, MSB_IDX)
                LSB_IDX                      -> lsb = s.decodeLongElement(descriptor, LSB_IDX)
                CompositeDecoder.DECODE_DONE -> break@loop
                else                         -> throw SerializationException("Unknown index $i")
            }
        }
        s.endStructure(descriptor)
        requireNotNull(msb)
        requireNotNull(lsb)
        return UUID(msb, lsb)
    }

    override fun serialize(encoder: Encoder, value: UUID) {
        val s = encoder.beginStructure(descriptor)
        s.encodeLongElement(descriptor, MSB_IDX, value.mostSignificantBits)
        s.encodeLongElement(descriptor, LSB_IDX, value.leastSignificantBits)
        s.endStructure(descriptor)
    }

    private const val MSB = "MSB"
    private const val LSB = "LSB"

    private const val MSB_IDX = 0
    private const val LSB_IDX = 1
}

object BigIntegerSerializer: KSerializer<BigInteger> {
    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("BigInteger") {
        element("bytes", ByteArraySerializer().descriptor)
    }

    override fun deserialize(decoder: Decoder): BigInteger {
        val s = decoder.beginStructure(descriptor)
        var bytes: ByteArray? = null
        if (s.decodeSequentially()) {
            bytes = s.decodeSerializableElement(descriptor, 0, ByteArraySerializer())
        } else loop@ while (true) {
            when (val i = s.decodeElementIndex(descriptor)) {
                0 -> bytes = s.decodeSerializableElement(descriptor, 0, ByteArraySerializer())
                CompositeDecoder.DECODE_DONE -> break@loop
                else                         -> throw SerializationException("Unknown index $i")
            }
        }
        s.endStructure(descriptor)
        requireNotNull(bytes)
        return BigInteger(bytes)
    }

    override fun serialize(encoder: Encoder, value: BigInteger) {
        val s = encoder.beginStructure(descriptor)
        s.encodeSerializableElement(descriptor, 0, ByteArraySerializer(), value.toByteArray())
        s.endStructure(descriptor)
    }
}

object BigDecimalSerializer: KSerializer<BigDecimal> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("BigDecimal", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): BigDecimal {
        return BigDecimal(decoder.decodeString())
    }

    override fun serialize(encoder: Encoder, value: BigDecimal) {
        encoder.encodeString(value.toString())
    }
}

object InstantSerializer: KSerializer<Instant> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Instant", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): Instant {
        return Instant.parse(decoder.decodeString())
    }

    override fun serialize(encoder: Encoder, value: Instant) {
        encoder.encodeString(value.toString())
    }
}
