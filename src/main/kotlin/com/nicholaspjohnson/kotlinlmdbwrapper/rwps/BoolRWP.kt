package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class BoolRWP<M: BaseLMDBObject<M>, T>(private val index: Int) : ReadWriteProperty<M, T> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setBool(index, value as Boolean)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getBool(index) as T
    }
}
