package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import org.lwjgl.system.MemoryStack
import java.nio.ByteBuffer

/**
 * Used to serialize database keys. Since keys generally require binary serialization that gives the same item no
 * matter which serialization format is being used (e.g. ProtoBuf Fixed vs. VarInt), these are separate from normal
 * serializers.
 *
 * Used to serialize [KeyType].
 */
interface KeySerializer<KeyType> {
    /**
     * Whether or not this is a const-size key serializer. If this is true, we may be able to set optimizations in the
     * DBI upon opening to increase DB access speed.
     */
    val isConstSize: Boolean

    /**
     * Whether or not this key serializer requires reverse key searching. For items like UUID's, this will be true on
     * little endian platforms for maximum COMB UUID speedups.
     */
    val needsReverseKey: Boolean

    /**
     * The key size if [isConstSize] is true, otherwise any value. If this is 4 or 8, we may be able to increase DB
     * speeds.
     */
    val keySize: Int

    /**
     * Serializes the given [key] into a [ByteBuffer], which can be allocated on the [stack].
     */
    fun serialize(key: KeyType, stack: MemoryStack): ByteBuffer

    /**
     * Deserializes the given [keyBytes] into a [KeyType].
     */
    fun deserialize(keyBytes: ByteBuffer): KeyType
}