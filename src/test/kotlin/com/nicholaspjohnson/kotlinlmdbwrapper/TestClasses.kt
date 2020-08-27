package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBSerDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBSerObject
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.serializer
import org.lwjgl.util.lmdb.LMDB
import java.util.*
import kotlin.collections.ArrayList

@Serializable
class TestObj(override var key: Int, var data: Int? = null) : LMDBSerObject<TestObj, Int>(Companion) {
    companion object : LMDBSerDbi<TestObj, Int>(serializer(), Int.serializer())
}

@Serializable
class MixNormalNulls(override var key: Int, var nullableInt: Int? = null, var aNullableString: String? = null, var normalString: String) : LMDBSerObject<MixNormalNulls, Int>(Companion) {
    companion object : LMDBSerDbi<MixNormalNulls, Int>(serializer(), Int.serializer())
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
) : LMDBSerObject<AllTypesObject, Long>(Companion) {
    companion object : LMDBSerDbi<AllTypesObject, Long>(serializer(), Long.serializer())
}

@Serializable
class MultiWrite(
    override var key: Long,
    var data: Long
) : LMDBSerObject<MultiWrite, Long>(Companion) {
    companion object : LMDBSerDbi<MultiWrite, Long>(serializer(), Long.serializer(), flags = LMDB.MDB_INTEGERKEY)
}

@Serializable
class ListTester(
    override var key: Long,
    var list: ArrayList<String>
) : LMDBSerObject<ListTester, Long>(Companion) {
    companion object : LMDBSerDbi<ListTester, Long>(serializer(), Long.serializer(), name = "list_tester", flags = LMDB.MDB_INTEGERKEY)
}
