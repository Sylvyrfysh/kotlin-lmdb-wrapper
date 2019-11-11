package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import java.nio.ByteBuffer

/**
 * A companion object that allows the class this represents to be used for collections and maps.
 */
interface RWPCompanion<M: AbstractRWP<*, T>, T> {
    /**
     * Writes [item] to [buffer] at [offset].
     */
    fun compWriteFn(buffer: ByteBuffer, offset: Int, item: T?)

    /**
     * Reads and returns an item from [buffer] at [offset.
     */
    fun compReadFn(buffer: ByteBuffer, offset: Int): T

    /**
     * Returns the size of [item] on disk.
     */
    fun compSizeFn(item: T?): Int
}