package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class CharRWP<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>, private val name: String) : SpecialRWP<M> {
    private val index by lazy { obj.getInd(name) }
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setChar(index, value as Char)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getChar(index) as T
    }
}
