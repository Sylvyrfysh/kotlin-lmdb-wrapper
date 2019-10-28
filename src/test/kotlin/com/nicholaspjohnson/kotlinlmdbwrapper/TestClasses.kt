package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer

class TestObj(data: ObjectBufferType): BaseLMDBObject<TestObj>(data) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key.toLong())
    }

    var key: Int by db
    var data: Int? by db
}

class AllTypesObject: BaseLMDBObject<AllTypesObject>(ObjectBufferType.None) {
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
    @VarChar(100)
    var varchar: String by db
}

class MixNormalNulls: BaseLMDBObject<MixNormalNulls>(ObjectBufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, normInt.toLong())
    }

    var normInt: Int by db
    var nullableInt: Int? by db
    @VarChar(100)
    var aNullableString: String? by db
    @VarChar(100)
    var normalString: String by db
}

class MultipleVarLongs: BaseLMDBObject<MultipleVarLongs>(ObjectBufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, first)
    }

    @VarLong
    var first: Long by db
    @VarLong
    var second: Long by db
}

class ByteArrayTesterObject: BaseLMDBObject<ByteArrayTesterObject>(ObjectBufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var buffer: ByteArray by db
    var zInt: Int by db
}

class DefaultSetTesterObject: BaseLMDBObject<DefaultSetTesterObject>(ObjectBufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var shouldBeDefault: Long by db

    init {
        set(this::shouldBeDefault, initialSet)
    }

    companion object {
        val initialSet = 129834765L
    }
}

class MisalignedShortArray: BaseLMDBObject<MisalignedShortArray>(ObjectBufferType.None) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key)
    }

    var key: Long by db
    var single: Byte by db
    var zArray: ShortArray by db
}