package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import kotlin.text.Charsets.UTF_8

sealed class UUIDKeySerializer: KeySerializer<UUID> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
    override val keySize: Int = Long.SIZE_BYTES * 2

    protected val uuidBuffer: ThreadLocal<ByteBuffer> =
        ThreadLocal.withInitial { ByteBuffer.allocate(keySize).order(ByteOrder.nativeOrder()) }

    private object UUIDKeySerializerLittleEndian: UUIDKeySerializer() {
        override fun serialize(key: UUID): ByteArray {
            val buf = uuidBuffer.get()

            buf.putLong(8, key.mostSignificantBits)
            buf.putLong(0, key.leastSignificantBits)
            return buf.array()
        }

        override fun deserialize(keyBytes: ByteArray): UUID {
            val buf = uuidBuffer.get()

            buf.position(0)
            buf.put(keyBytes, 0, 16)
            return UUID(buf.getLong(8), buf.getLong(0))
        }
    }

    private object UUIDKeySerializerBigEndian: UUIDKeySerializer() {
        override fun serialize(key: UUID): ByteArray {
            val buf = uuidBuffer.get()

            buf.putLong(0, key.mostSignificantBits)
            buf.putLong(8, key.leastSignificantBits)
            return buf.array()
        }

        override fun deserialize(keyBytes: ByteArray): UUID {
            val buf = uuidBuffer.get()

            buf.position(0)
            buf.put(keyBytes, 0, 16)
            return UUID(buf.getLong(0), buf.getLong(8))
        }
    }

    companion object {
        operator fun invoke(): UUIDKeySerializer {
            return if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                UUIDKeySerializerLittleEndian
            } else {
                UUIDKeySerializerBigEndian
            }
        }
    }
}

object LongKeySerializer: KeySerializer<Long> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = false
    override val keySize: Int = Long.SIZE_BYTES

    private val longBuffer: ThreadLocal<ByteBuffer> =
        ThreadLocal.withInitial { ByteBuffer.allocate(keySize).order(ByteOrder.nativeOrder()) }

    override fun serialize(key: Long): ByteArray {
        val buf = longBuffer.get()

        buf.putLong(0, key)
        return buf.array()
    }

    override fun deserialize(keyBytes: ByteArray): Long {
        val buf = longBuffer.get()

        buf.position(0)
        buf.put(keyBytes, 0, 8)
        return buf.getLong(0)
    }
}

object IntKeySerializer: KeySerializer<Int> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = false
    override val keySize: Int = Int.SIZE_BYTES

    private val longBuffer: ThreadLocal<ByteBuffer> =
        ThreadLocal.withInitial { ByteBuffer.allocate(keySize).order(ByteOrder.nativeOrder()) }

    override fun serialize(key: Int): ByteArray {
        val buf = longBuffer.get()

        buf.putInt(0, key)
        return buf.array()
    }

    override fun deserialize(keyBytes: ByteArray): Int {
        val buf = longBuffer.get()

        buf.position(0)
        buf.put(keyBytes, 0, 8)
        return buf.getInt(0)
    }
}

object StringKeySerializer: KeySerializer<String> {
    override val isConstSize: Boolean = false
    override val needsReverseKey: Boolean = false
    override val keySize: Int = -1

    override fun serialize(key: String): ByteArray {
        return key.toByteArray(UTF_8)
    }

    override fun deserialize(keyBytes: ByteArray): String {
        return keyBytes.toString(UTF_8)
    }
}