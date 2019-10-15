package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

class ShortTypedRWP<M: BaseLMDBObject<M>, T, R>(private val index: Int, private val convertTo: (T) -> R, private val convertFrom: (R) -> T) : ReadWriteProperty<M, R> {
    override fun setValue(thisRef: M, property: KProperty<*>, value: R) {
        thisRef.setShort(index, convertFrom(value) as Short)
    }

    override fun getValue(thisRef: M, property: KProperty<*>): R {
        return convertTo(thisRef.getShort(index) as T)
    }
}