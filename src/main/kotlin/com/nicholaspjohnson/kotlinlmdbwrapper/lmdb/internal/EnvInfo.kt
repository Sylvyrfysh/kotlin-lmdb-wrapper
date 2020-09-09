package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.internal

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.StringKeySerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer

@Serializable
internal class EnvInfo(
    override var key: String,
    val flags: Int,
    val isVersioned: Boolean,
) : LMDBObject<EnvInfo, String>(Companion) {
    inline val name: String
        get() = key

    companion object : LMDBDbi<EnvInfo, String>(serializer(), StringKeySerializer, name = "__env_info")
}