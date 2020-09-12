package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.MDBCmpFuncI
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.util.*

/**
 * Serializes UUID's into keys.
 */
sealed class UUIDKeySerializer: KeySerializer<UUID> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN
    override val keySize: Int = Long.SIZE_BYTES * 2
    override val needsFree: Boolean = false

    protected val buffer = ThreadLocal.withInitial { MemoryUtil.memAlloc(keySize).order(ByteOrder.nativeOrder()) }

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            MemoryUtil.memFree(buffer.get())
        })
    }

    private object UUIDKeySerializerLittleEndian : UUIDKeySerializer() {
        override fun serialize(key: UUID): ByteBuffer {
            val buf = buffer.get()

            buf.putLong(8, key.mostSignificantBits)
            buf.putLong(0, key.leastSignificantBits)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): UUID {
            return UUID(keyBytes.getLong(8), keyBytes.getLong(0))
        }
    }

    private object UUIDKeySerializerBigEndian: UUIDKeySerializer() {
        override fun serialize(key: UUID): ByteBuffer {
            val buf = buffer.get()

            buf.putLong(0, key.mostSignificantBits)
            buf.putLong(8, key.leastSignificantBits)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): UUID {
            return UUID(keyBytes.getLong(0), keyBytes.getLong(8))
        }
    }

    companion object {
        /**
         * Returns the proper serializer for the current endianness.
         */
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
    override val needsFree: Boolean = false

    private val buffer = ThreadLocal.withInitial { MemoryUtil.memAlloc(keySize).order(ByteOrder.nativeOrder()) }

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            MemoryUtil.memFree(buffer.get())
        })
    }

    override fun serialize(key: Long): ByteBuffer {
        val buf = buffer.get()

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
    override val needsFree: Boolean = false

    private val buffer = ThreadLocal.withInitial { MemoryUtil.memAlloc(keySize).order(ByteOrder.nativeOrder()) }

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            MemoryUtil.memFree(buffer.get())
        })
    }

    override fun serialize(key: Int): ByteBuffer {
        val buf = buffer.get()

        buf.putInt(0, key)
        return buf
    }

    override fun deserialize(keyBytes: ByteBuffer): Int {
        return keyBytes.getInt(0)
    }
}

/**
 * Serializes [String]s into key buffers using the ASCII encoding. Compared to the [ComplexStringKeySerializer], it
 * uses less time computing where each item goes as it does not need a custom comparator so that the strings are
 * properly compared.
 */
object SimpleStringKeySerializer : KeySerializer<String> {
    override val isConstSize: Boolean = false
    override val needsReverseKey: Boolean = false
    override val keySize: Int = -1
    override val needsFree: Boolean = true

    override fun serialize(key: String): ByteBuffer {
        return MemoryUtil.memASCII(key, false)
    }

    override fun deserialize(keyBytes: ByteBuffer): String {
        return MemoryUtil.memASCII(keyBytes)
    }
}

/**
 * Serializes [String]s into key buffers using the UTF-8 encoding. Compared to the [SimpleStringKeySerializer], it uses
 * more time computing where each item goes as it uses a custom comparator so that the strings are properly compared.
 */
object ComplexStringKeySerializer : KeySerializer<String> {
    override val isConstSize: Boolean = false
    override val needsReverseKey: Boolean = false
    override val keySize: Int = -1
    override val needsFree: Boolean = true
    override val comparator: MDBCmpFuncI? = MDBCmpFuncI { a, b ->
        val actA = MDBVal.create(a)
        val actB = MDBVal.create(b)
        return@MDBCmpFuncI deserialize(actA.mv_data()!!).compareTo(deserialize(actB.mv_data()!!))
    }

    override fun serialize(key: String): ByteBuffer {
        return MemoryUtil.memUTF8(key, false)
    }

    override fun deserialize(keyBytes: ByteBuffer): String {
        return MemoryUtil.memUTF8(keyBytes)
    }
}

/**
 * Serializes [Instant]s into key buffers.
 */
sealed class InstantKeySerializer: KeySerializer<Instant> {
    override val isConstSize: Boolean = true
    override val needsReverseKey: Boolean = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
    override val keySize: Int = Long.SIZE_BYTES + Int.SIZE_BYTES
    override val needsFree: Boolean = false

    protected val buffer =
        ThreadLocal.withInitial { MemoryUtil.memAlloc(IntKeySerializer.keySize).order(ByteOrder.nativeOrder()) }

    init {
        Runtime.getRuntime().addShutdownHook(Thread {
            MemoryUtil.memFree(buffer.get())
        })
    }

    private object InstantKeySerializerLittleEndian : InstantKeySerializer() {
        override fun serialize(key: Instant): ByteBuffer {
            val buf = buffer.get()

            buf.putLong(4, key.epochSecond)
            buf.putInt(0, key.nano)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): Instant {
            return Instant.ofEpochSecond(keyBytes.getLong(4), keyBytes.getInt(0).toLong())
        }
    }

    private object InstantKeySerializerBigEndian: InstantKeySerializer() {
        override fun serialize(key: Instant): ByteBuffer {
            val buf = buffer.get()

            buf.putLong(0, key.epochSecond)
            buf.putInt(8, key.nano)
            return buf
        }

        override fun deserialize(keyBytes: ByteBuffer): Instant {
            return Instant.ofEpochSecond(keyBytes.getLong(0), keyBytes.getInt(8).toLong())
        }
    }

    companion object {
        /**
         * Returns the proper serializer for the current endianness.
         */
        operator fun invoke(): InstantKeySerializer {
            return if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                InstantKeySerializerLittleEndian
            } else {
                InstantKeySerializerBigEndian
            }
        }
    }
}