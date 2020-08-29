package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.IntKeySerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.LongKeySerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.serializer
import org.lwjgl.util.lmdb.LMDB
import kotlin.collections.ArrayList

@Serializable
class TestObj(override var key: Int, var data: Int? = null) : LMDBObject<TestObj, Int>(Companion) {
    companion object : LMDBDbi<TestObj, Int>(serializer(), IntKeySerializer)
}

@Serializable
class MixNormalNulls(override var key: Int, var nullableInt: Int? = null, var aNullableString: String? = null, var normalString: String) : LMDBObject<MixNormalNulls, Int>(Companion) {
    companion object : LMDBDbi<MixNormalNulls, Int>(serializer(), IntKeySerializer)
}

@Serializable
class AllTypesObject(
    var bool: Boolean,
    var byte: Byte,
    var short: Short,
    var char: Char,
    var int: Int,
    var float: Float,
    override var key: Long,
    var double: Double,
    var varchar: String,
    var nullableInt: Int? = null
) : LMDBObject<AllTypesObject, Long>(Companion) {
    companion object : LMDBDbi<AllTypesObject, Long>(serializer(), LongKeySerializer)
}

@Serializable
class MultiWrite(
    override var key: Long,
    var data: Long
) : LMDBObject<MultiWrite, Long>(Companion) {
    companion object : LMDBDbi<MultiWrite, Long>(serializer(), LongKeySerializer, flags = LMDB.MDB_INTEGERKEY)
}

@Serializable
class ListTester(
    override var key: Long,
    var list: ArrayList<String>
) : LMDBObject<ListTester, Long>(Companion) {
    companion object : LMDBDbi<ListTester, Long>(serializer(), LongKeySerializer, name = "list_tester", flags = LMDB.MDB_INTEGERKEY)
}
