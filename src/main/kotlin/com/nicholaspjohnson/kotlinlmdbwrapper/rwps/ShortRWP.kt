package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class ShortRWP<M: BaseLMDBObject<M>>(private var index: Int) : SpecialRWP<M> {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setShort(index, value as Short)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getShort(index) as T
    }
}
