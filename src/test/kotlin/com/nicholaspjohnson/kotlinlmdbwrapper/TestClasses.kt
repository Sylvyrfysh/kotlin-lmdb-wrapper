package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import org.lwjgl.util.lmdb.LMDB
import java.nio.ByteBuffer

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

/*
class ByteArrayTesterObject: BaseLMDBObject<ByteArrayTesterObject>(BufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var buffer: ByteArray by db
    var zInt: Int by db
}

class DefaultSetTesterObject: BaseLMDBObject<DefaultSetTesterObject>(BufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var shouldBeDefault: Long by db

    init {
        set(this::shouldBeDefault, initialSet)
    }

    companion object {
        const val initialSet = 129834765L
    }
}

class MisalignedShortArray(from: BufferType): BaseLMDBObject<MisalignedShortArray>(from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var single: Byte by db
    var zArray: ShortArray by db
}

class ListTester(from: BufferType): BaseLMDBObject<ListTester>(from) {
    constructor(): this(BufferType.None)
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var list: ArrayList<String> by db.collection(this::list) { ArrayList() }
}

class CustomUUIDRWP: BaseLMDBObject<CustomUUIDRWP>(BufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var uuid: UUID by db
}

class MapTester: BaseLMDBObject<MapTester>(BufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var map: HashMap<String, Int> by db.map(this::map) { HashMap() }
}
 */