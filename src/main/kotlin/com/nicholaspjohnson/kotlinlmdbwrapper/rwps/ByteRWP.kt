package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class ByteRWP<M: BaseLMDBObject<M>>(private val index: Int) : SpecialRWP<M> {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setByte(index, value as Byte)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getByte(index) as T
    }
}
