package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

/**
 * A default [Boolean] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [AbstractRWP]
 */
class BoolRWP<M: BaseLMDBObject<M>>(lmdbObject: BaseLMDBObject<M>, propertyName: String): AbstractRWP<M>(lmdbObject, propertyName) {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setBool(index, value as Boolean?)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getBool(index) as T
    }
}
