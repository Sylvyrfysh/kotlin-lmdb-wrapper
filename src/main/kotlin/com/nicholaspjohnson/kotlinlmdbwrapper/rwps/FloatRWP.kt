package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class FloatRWP<M: BaseLMDBObject<M>>(private val index: Int) : SpecialRWP<M> {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setFloat(index, value as Float)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getFloat(index) as T
    }
}
