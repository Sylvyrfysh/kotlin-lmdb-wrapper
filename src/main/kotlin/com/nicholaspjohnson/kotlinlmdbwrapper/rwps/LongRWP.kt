package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class LongRWP<M: BaseLMDBObject<M>, T>(private val index: Int) : ReadWriteProperty<M, T> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setLong(index, value as Long)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getLong(index) as T
    }
}
