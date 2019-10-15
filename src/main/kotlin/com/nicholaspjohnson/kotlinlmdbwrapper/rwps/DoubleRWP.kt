package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class DoubleRWP<M: BaseLMDBObject<M>, T>(private val index: Int) : ReadWriteProperty<M, T> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setDouble(index, value as Double)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getDouble(index) as T
    }
}
