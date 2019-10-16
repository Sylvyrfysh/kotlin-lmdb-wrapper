package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject

/**
 * A base delegate for a RWP that will get [index] through [lmdbObject] and [propertyName]
 *
 * @param[lmdbObject] Class of type [M] to operate on
 * @param[propertyName] The property name to gove an RWP for
 *
 * @constructor
 * Set up the RWP with the passed in with the underlying object [lmdbObject] and the property name [propertyName]
 *
 */
abstract class AbstractRWP<M: BaseLMDBObject<M>>(private val lmdbObject: BaseLMDBObject<M>, private val propertyName: String) : RWPInterface<M> {
    /**
     * Evaluates the index on first get.
     */
    protected val index by lazy { lmdbObject.getInd(propertyName) }
}