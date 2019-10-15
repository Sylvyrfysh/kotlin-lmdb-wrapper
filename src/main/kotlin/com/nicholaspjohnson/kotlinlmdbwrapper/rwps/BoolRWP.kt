package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class BoolRWP<M: BaseLMDBObject<M>>(private val index: Int) : SpecialRWP<M> {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setBool(index, value as Boolean)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getBool(index) as T
    }
}
