package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

/**
 * A ReadWrite delegate for a given property inside the class [M]
 */
interface RWPInterface<M: BaseLMDBObject<M>> {
    /**
     * Returns the underlying value of [property] in the class [M]'s [thisRef] instance
     */
    operator fun <T> getValue(thisRef: M, property: KProperty<*>): T
    /**
     * Sets the underlying value of [property] in the class [M]'s [thisRef] instance to [value]]
     */
    operator fun <T> setValue(thisRef: M, property: KProperty<*>, value: T)
}