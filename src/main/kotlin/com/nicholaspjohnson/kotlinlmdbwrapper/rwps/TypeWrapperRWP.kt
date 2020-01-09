package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import java.nio.ByteBuffer
import kotlin.reflect.KProperty

/**
 * An object that does not do any writing, instead delegating everything to [underlying] with type conversions through [fromObjToDB] and [fromDBToObj].
 *
 * @param M The class type we'll write to
 * @param R The object type that the user will have access to
 * @param D The object type this RWP is on DB
 * @property underlying The underlying RWP for DB uses.
 * @property fromDBToObj The function to turn the DB object into a user object.
 * @property fromObjToDB The function to turn the user object into a DB object.
 * @constructor
 * Creates a new type-wrapping RWP for a data member in [lmdbObject] that is [nullable].
 *
 * @param lmdbObject The object this RWP has a data member for.
 * @param nullable If this object is nullable or not.
 */
@Suppress("UNCHECKED_CAST")
class TypeWrapperRWP<M: BaseLMDBObject<M>, R, D>(private val underlying: AbstractRWP<M, D?>, private val fromDBToObj: (D) -> R, private val fromObjToDB: (R) -> D, lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    AbstractRWP<M, R>(lmdbObject, nullable) {
    override val getSize: (R) -> Int = underlying.getSize as (R) -> Int

    override fun writeToDB(
        writeBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int {
        return underlying.writeToDB(writeBuffer, startingOffset, nullStoreOption)
    }

    override fun readFromDB(
        readBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int {
        return underlying.readFromDB(readBuffer, startingOffset, nullStoreOption)
    }

    override fun <T> getValue(thisRef: BaseLMDBObject<M>, property: KProperty<*>): T {
        @Suppress("UNCHECKED_CAST")
        return fromDBToObj(underlying.getValue(thisRef, property)) as T
    }

    override fun <T> setValue(thisRef: BaseLMDBObject<M>, property: KProperty<*>, value: T) {
        @Suppress("UNCHECKED_CAST")
        underlying.setValue(thisRef, property, value?.let { fromObjToDB(it as R) })
    }
}