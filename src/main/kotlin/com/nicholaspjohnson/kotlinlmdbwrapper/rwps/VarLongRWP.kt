package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

class VarLongRWP<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>, private val name: String) : SpecialRWP<M> {
    private val index by lazy { obj.getInd(name) }
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setVarLong(index, value as Long)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getVarLong(index) as T
    }
}