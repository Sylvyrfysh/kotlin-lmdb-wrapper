package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

import org.lwjgl.util.lmdb.MDBCmpFuncI
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
     * Whether or not the returned [ByteBuffer] should be freed. Some items are allocated on the heap rather than with
     * constant sized thread-local buffers, and this should be true for this items.
     */
    val needsFree: Boolean

    /**
     *
     */
    val comparator: MDBCmpFuncI?
        get() = null

    /**
     * Serializes the given [key] into a [ByteBuffer]
     */
    fun serialize(key: KeyType): ByteBuffer

    /**
     * Deserializes the given [keyBytes] into a [KeyType].
     */
    fun deserialize(keyBytes: ByteBuffer): KeyType
}