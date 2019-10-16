package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import kotlin.reflect.KProperty

/**
 * A default [Long] RWP that will act on instances of the class [M].
 * Writes and reads data as a VarLong internally
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [AbstractRWP]
 */
class VarLongRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String): AbstractRWP<M>(obj, name) {
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        thisRef.setVarLong(index, value as Long)
    }

    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return thisRef.getVarLong(index) as T
    }
}
