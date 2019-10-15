package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class ShortRWP<M: BaseLMDBObject<M>, T>(private var index: Int) : ReadWriteProperty<M, T> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setShort(index, value as Short)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getShort(index) as T
    }
}
