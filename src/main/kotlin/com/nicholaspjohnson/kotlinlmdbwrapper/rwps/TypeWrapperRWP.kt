package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer
import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
class TypeWrapperRWP<M: BaseLMDBObject<M>, R, D>(private val underlying: AbstractRWP<M, D?>, private val fromDBToObj: (D) -> R, private val fromObjToDB: (R) -> D, lmdbObject: BaseLMDBObject<M>, nullable: Boolean) :
    AbstractRWP<M, R>(lmdbObject, nullable) {
    override val getSize: (R) -> Int = underlying.getSize as (R) -> Int

    override fun writeToDB(writeBuffer: ByteBuffer, startingOffset: Int): Int {
        return underlying.writeToDB(writeBuffer, startingOffset)
    }

    override fun readFromDB(readBuffer: ByteBuffer, startingOffset: Int): Int {
        return underlying.readFromDB(readBuffer, startingOffset)
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