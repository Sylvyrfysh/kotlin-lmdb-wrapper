package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class LongRWP<M: BaseLMDBObject<M>>(private val index: Int) : SpecialRWP<M> {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setLong(index, value as Long)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getLong(index) as T
    }
}
