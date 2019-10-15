package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class IntRWP<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>, private val name: String) : SpecialRWP<M> {
    private val index by lazy { obj.getInd(name) }
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setInt(index, value as Int)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getInt(index) as T
    }
}
