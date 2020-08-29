package com.nicholaspjohnson.kotlinlmdbwrapper.serializers

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

    fun serialize(key: KeyType): ByteArray

    fun deserialize(keyBytes: ByteArray): KeyType
}