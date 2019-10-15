package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

interface SpecialRWP<M: BaseLMDBObject<M>> {
    operator fun <T> getValue(thisRef: M, property: KProperty<*>): T
    operator fun <T> setValue(thisRef: M, property: KProperty<*>, value: T)
}