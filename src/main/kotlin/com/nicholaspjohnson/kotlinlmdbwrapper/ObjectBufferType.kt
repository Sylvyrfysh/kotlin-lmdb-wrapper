package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer

sealed class ObjectBufferType {
    data class Buffer(val buffer: ByteBuffer) : ObjectBufferType()
    data class DBRead(val buffer: ByteBuffer) : ObjectBufferType()
    object New : ObjectBufferType()
}