package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.map.MapRWP
import org.lwjgl.util.lmdb.LMDB
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class TestObj(data: BufferType): BaseLMDBObject<TestObj>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key.toLong())
    }

    var key: Int by db
    var data: Int? by db

    companion object: LMDBDbi<TestObj>("test_obj", NullStoreOption.SPEED, ::TestObj, LMDB.MDB_INTEGERKEY)
}

class MixNormalNulls(data: BufferType): BaseLMDBObject<MixNormalNulls>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, normInt.toLong())
    }

    var normInt: Int by db
    var nullableInt: Int? by db
    var aNullableString: String? by db
    var normalString: String by db

    companion object: LMDBDbi<MixNormalNulls>("mix_normal_nulls", NullStoreOption.SIZE, ::MixNormalNulls, LMDB.MDB_INTEGERKEY)
}

class AllTypesObject(data: BufferType): BaseLMDBObject<AllTypesObject>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, long)
    }

    var bool: Boolean by db
    var byte: Byte by db
    var short: Short by db
    var char: Char by db
    var int: Int by db
    var float: Float by db
    var long: Long by db
    var double: Double by db
    @VarLong
    var varlong: Long by db
    var varchar: String by db
    var nullableInt: Int? by db

    companion object: LMDBDbi<AllTypesObject>("all_types_object", NullStoreOption.SPEED, ::AllTypesObject, LMDB.MDB_INTEGERKEY)
}

class MultipleVarLongs(data: BufferType): BaseLMDBObject<MultipleVarLongs>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, first)
    }

    @VarLong
    var first: Long by db
    @VarLong
    var second: Long by db

    companion object: LMDBDbi<MultipleVarLongs>("multi_varlongs", NullStoreOption.SIZE, ::MultipleVarLongs, LMDB.MDB_INTEGERKEY)
}

class MultiWrite(data: BufferType): BaseLMDBObject<MultiWrite>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var data: Long by db

    companion object: LMDBDbi<MultiWrite>("multi_write", NullStoreOption.SPEED, ::MultiWrite, LMDB.MDB_INTEGERKEY)
}

class DefaultSetTesterObject(data: BufferType): BaseLMDBObject<DefaultSetTesterObject>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var shouldBeDefault: Long by db

    init {
        set(DefaultSetTesterObject::shouldBeDefault, initialSet)
    }

    companion object: LMDBDbi<DefaultSetTesterObject>("default_set", NullStoreOption.SPEED, ::DefaultSetTesterObject, LMDB.MDB_INTEGERKEY) {
        const val initialSet = 129834765L
    }
}

class ByteArrayTesterObject(data: BufferType): BaseLMDBObject<ByteArrayTesterObject>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var buffer: ByteArray by db
    var zInt: Int by db
    companion object: LMDBDbi<ByteArrayTesterObject>("byte_array_tester", NullStoreOption.SIZE, ::ByteArrayTesterObject, LMDB.MDB_INTEGERKEY)
}

class MisalignedShortArray(from: BufferType): BaseLMDBObject<MisalignedShortArray>(Companion, from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var single: Byte by db
    var zArray: ShortArray by db

    companion object: LMDBDbi<MisalignedShortArray>("misaligned_short_array", NullStoreOption.SIZE, ::MisalignedShortArray, LMDB.MDB_INTEGERKEY)
}

class ListTester(from: BufferType): BaseLMDBObject<ListTester>(Companion, from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var list: ArrayList<String> by db.collection(ListTester::list) { ArrayList() }

    companion object: LMDBDbi<ListTester>("list_tester", NullStoreOption.SIZE, ::ListTester, LMDB.MDB_INTEGERKEY)
}

class CustomUUIDRWP(from: BufferType): BaseLMDBObject<CustomUUIDRWP>(Companion, from) {
    constructor() : this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var uuid: UUID by db

    companion object: LMDBDbi<CustomUUIDRWP>("custom_uuid_rwp", NullStoreOption.SIZE, ::CustomUUIDRWP, LMDB.MDB_INTEGERKEY)
}

class MapTester(from: BufferType): BaseLMDBObject<MapTester>(Companion, from) {
    constructor() : this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var map: HashMap<String, Int> by db.map(MapTester::map) { HashMap() }

    companion object: LMDBDbi<MapTester>("map_tester", NullStoreOption.SIZE, ::MapTester, LMDB.MDB_INTEGERKEY)
}

class NoRWP(from: BufferType): BaseLMDBObject<NoRWP>(Companion, from) {
    constructor() : this(BufferType.None)

    override fun keyFunc(keyBuffer: ByteBuffer) {}

    var none: ByteArrayOutputStream by db // Sufficiently ridiculous to never make serializable

    companion object: LMDBDbi<NoRWP>("no_rwp", NullStoreOption.SIZE, ::NoRWP, LMDB.MDB_INTEGERKEY)
}