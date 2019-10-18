package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

/**
 * A default [Short] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [AbstractRWP]
 */
class ShortRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String): AbstractRWP<M>(obj, name) {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setShort(index, value as Short?)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getShort(index) as T
    }
}
