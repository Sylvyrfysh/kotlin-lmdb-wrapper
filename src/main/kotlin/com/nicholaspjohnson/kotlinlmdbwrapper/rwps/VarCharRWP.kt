package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class VarCharRWP<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>, private val name: String) : SpecialRWP<M> {
    private val index by lazy { obj.getInd(name) }
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setVarChar(index, value as String)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getVarChar(index) as T
    }
}