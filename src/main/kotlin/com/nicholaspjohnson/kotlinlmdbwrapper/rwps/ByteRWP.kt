package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class ByteRWP<M: BaseLMDBObject<M>, T>(private val index: Int) : ReadWriteProperty<M, T> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setByte(index, value as Byte)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getByte(index) as T
    }
}
