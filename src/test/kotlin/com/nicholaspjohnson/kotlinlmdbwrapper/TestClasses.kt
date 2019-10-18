package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer

class TestObj(data: ObjectBufferType): BaseLMDBObject<TestObj>(data) {
    override fun keyFunc(keyBuffer: ByteBuffer) {
        keyBuffer.putLong(0, key.toLong())
    }

    var key: Int by db
    var data: Int? by db
}

class AllTypesObject: BaseLMDBObject<AllTypesObject>(ObjectBufferType.New) {
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
