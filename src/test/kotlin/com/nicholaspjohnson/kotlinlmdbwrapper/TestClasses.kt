package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import org.lwjgl.util.lmdb.LMDB
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class TestObj(data: BufferType): LMDBObject<TestObj>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key.toLong())
    }

    var key: Int by db
    var data: Int? by db

    companion object: LMDBDbi<TestObj>("test_obj", ::TestObj, NullStoreOption.SPEED, LMDB.MDB_INTEGERKEY)
}

class MixNormalNulls(data: BufferType): LMDBObject<MixNormalNulls>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, normInt.toLong())
    }

    var normInt: Int by db
    var nullableInt: Int? by db
    var aNullableString: String? by db
    var normalString: String by db

    companion object: LMDBDbi<MixNormalNulls>(
        "mix_normal_nulls",
        ::MixNormalNulls,
        NullStoreOption.SIZE,
        LMDB.MDB_INTEGERKEY
    )
}

class AllTypesObject(data: BufferType): LMDBObject<AllTypesObject>(Companion, data) {
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

    companion object: LMDBDbi<AllTypesObject>(
        "all_types_object",
        ::AllTypesObject,
        NullStoreOption.SPEED,
        LMDB.MDB_INTEGERKEY
    )
}

class MultipleVarLongs(data: BufferType): LMDBObject<MultipleVarLongs>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, first)
    }

    @VarLong
    var first: Long by db
    @VarLong
    var second: Long by db

    companion object: LMDBDbi<MultipleVarLongs>(
        "multi_varlongs",
        ::MultipleVarLongs,
        NullStoreOption.SIZE,
        LMDB.MDB_INTEGERKEY
    )
}

class MultiWrite(data: BufferType): LMDBObject<MultiWrite>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var data: Long by db

    companion object: LMDBDbi<MultiWrite>("multi_write", ::MultiWrite, NullStoreOption.SPEED, LMDB.MDB_INTEGERKEY)
}

class DefaultSetTesterObject(data: BufferType): LMDBObject<DefaultSetTesterObject>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var shouldBeDefault: Long by db

    init {
        set(DefaultSetTesterObject::shouldBeDefault, initialSet)
    }

    companion object: LMDBDbi<DefaultSetTesterObject>(
        "default_set",
        ::DefaultSetTesterObject,
        NullStoreOption.SPEED,
        LMDB.MDB_INTEGERKEY
    ) {
        const val initialSet = 129834765L
    }
}

class ByteArrayTesterObject(data: BufferType): LMDBObject<ByteArrayTesterObject>(Companion, data) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var buffer: ByteArray by db
    var zInt: Int by db
    companion object: LMDBDbi<ByteArrayTesterObject>(
        "byte_array_tester",
        ::ByteArrayTesterObject,
        NullStoreOption.SIZE,
        LMDB.MDB_INTEGERKEY
    )
}

class MisalignedShortArray(from: BufferType): LMDBObject<MisalignedShortArray>(Companion, from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var single: Byte by db
    var zArray: ShortArray by db

    companion object: LMDBDbi<MisalignedShortArray>(
        "misaligned_short_array",
        ::MisalignedShortArray,
        NullStoreOption.SIZE,
        LMDB.MDB_INTEGERKEY
    )
}

class ListTester(from: BufferType): LMDBObject<ListTester>(Companion, from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var list: ArrayList<String> by db.collection(ListTester::list) { ArrayList() }

    companion object: LMDBDbi<ListTester>("list_tester", ::ListTester, NullStoreOption.SIZE, LMDB.MDB_INTEGERKEY)
}

class CustomUUIDRWP(from: BufferType): LMDBObject<CustomUUIDRWP>(Companion, from) {
    constructor() : this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var uuid: UUID by db

    companion object: LMDBDbi<CustomUUIDRWP>(
        "custom_uuid_rwp",
        ::CustomUUIDRWP,
        NullStoreOption.SIZE,
        LMDB.MDB_INTEGERKEY
    )
}

class MapTester(from: BufferType): LMDBObject<MapTester>(Companion, from) {
    constructor() : this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var map: HashMap<String, Int> by db.map(MapTester::map) { HashMap() }

    companion object: LMDBDbi<MapTester>("map_tester", ::MapTester, NullStoreOption.SIZE, LMDB.MDB_INTEGERKEY)
}

class NoRWP(from: BufferType): LMDBObject<NoRWP>(Companion, from) {
    constructor() : this(BufferType.None)

    override fun keyFunc(keyBuffer: ByteBuffer) {}

    var none: ByteArrayOutputStream by db // Sufficiently ridiculous to never make serializable

    companion object: LMDBDbi<NoRWP>("no_rwp", ::NoRWP, NullStoreOption.SIZE, LMDB.MDB_INTEGERKEY)
}