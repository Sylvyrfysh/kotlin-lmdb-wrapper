package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import java.nio.ByteBuffer

interface RWPCompanion<M: AbstractRWP<*, T>, T> {
    fun compWriteFn(buffer: ByteBuffer, offset: Int, item: T?)
    fun compReadFn(buffer: ByteBuffer, offset: Int): T
    fun compSizeFn(item: T?): Int
}