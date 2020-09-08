package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.util.*

sealed class UUIDKeySerializer: KeySerializer<UUID> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
    override val keySize: Int = Long.SIZE_BYTES * 2

    private object UUIDKeySerializerLittleEndian: UUIDKeySerializer() {
        override fun serialize(key: UUID, stack: MemoryStack): ByteBuffer {
            val buf = stack.malloc(keySize)

            buf.putLong(8, key.mostSignificantBits)
            buf.putLong(0, key.leastSignificantBits)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): UUID {
            return UUID(keyBytes.getLong(8), keyBytes.getLong(0))
        }
    }

    private object UUIDKeySerializerBigEndian: UUIDKeySerializer() {
        override fun serialize(key: UUID, stack: MemoryStack): ByteBuffer {
            val buf = stack.malloc(keySize)

            buf.putLong(0, key.mostSignificantBits)
            buf.putLong(8, key.leastSignificantBits)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): UUID {
            return UUID(keyBytes.getLong(0), keyBytes.getLong(8))
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

    override fun serialize(key: Long, stack: MemoryStack): ByteBuffer {
        val buf = stack.malloc(keySize)

        buf.putLong(0, key)
        return buf
    }

    override fun deserialize(keyBytes: ByteBuffer): Long {
        return keyBytes.getLong(0)
    }
}

object IntKeySerializer: KeySerializer<Int> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = false
    override val keySize: Int = Int.SIZE_BYTES

    override fun serialize(key: Int, stack: MemoryStack): ByteBuffer {
        val buf = stack.malloc(keySize)

        buf.putInt(0, key)
        return buf
    }

    override fun deserialize(keyBytes: ByteBuffer): Int {
        return keyBytes.getInt(0)
    }
}

object StringKeySerializer: KeySerializer<String> {
    override val isConstSize: Boolean = false
    override val needsReverseKey: Boolean = false
    override val keySize: Int = -1

    override fun serialize(key: String, stack: MemoryStack): ByteBuffer {
        val bufSize = MemoryUtil.memLengthUTF8(key, false)
        val buf = stack.malloc(bufSize)
        MemoryUtil.memUTF8(key, false, buf)

        return buf
    }

    override fun deserialize(keyBytes: ByteBuffer): String {
        return MemoryUtil.memUTF8(keyBytes)
    }
}

sealed class InstantKeySerializer: KeySerializer<Instant> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
    override val keySize: Int = Long.SIZE_BYTES + Int.SIZE_BYTES

    private object InstantKeySerializerLittleEndian: InstantKeySerializer() {
        override fun serialize(key: Instant, stack: MemoryStack): ByteBuffer {
            val buf = stack.malloc(keySize)

            buf.putLong(4, key.epochSecond)
            buf.putInt(0, key.nano)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): Instant {
            return Instant.ofEpochSecond(keyBytes.getLong(4), keyBytes.getInt(0).toLong())
        }
    }

    private object InstantKeySerializerBigEndian: InstantKeySerializer() {
        override fun serialize(key: Instant, stack: MemoryStack): ByteBuffer {
            val buf = stack.malloc(keySize)

            buf.putLong(0, key.epochSecond)
            buf.putInt(8, key.nano)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): Instant {
            return Instant.ofEpochSecond(keyBytes.getLong(0), keyBytes.getInt(8).toLong())
        }
    }

    companion object {
        operator fun invoke(): InstantKeySerializer {
            return if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                InstantKeySerializerLittleEndian
            } else {
                InstantKeySerializerBigEndian
            }
        }
    }
}