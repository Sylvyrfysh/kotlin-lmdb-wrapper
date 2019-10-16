package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

/**
 * A default [String] RWP that will act on instances of the class [M]
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [AbstractRWP]
 */
class VarCharRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String): AbstractRWP<M>(obj, name) {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setVarChar(index, value as String)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getVarChar(index) as T
    }
}
