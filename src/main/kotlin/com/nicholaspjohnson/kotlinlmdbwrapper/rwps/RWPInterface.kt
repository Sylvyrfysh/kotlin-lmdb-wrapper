package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import kotlin.reflect.KProperty

/**
 * A ReadWrite delegate for a given property inside the class [M]
 */
interface RWPInterface<M: LMDBObject<M>> {
    /**
     * Returns the underlying value of [property] in the class [M]'s [thisRef] instance
     */
    operator fun <T> getValue(thisRef: LMDBObject<M>, property: KProperty<*>): T
    /**
     * Sets the underlying value of [property] in the class [M]'s [thisRef] instance to [value]]
     */
    operator fun <T> setValue(thisRef: LMDBObject<M>, property: KProperty<*>, value: T)

    /**
     * Returns the size of this object when it is written to the DB
     */
    fun getDiskSize(nullStoreOption: NullStoreOption): Int
}