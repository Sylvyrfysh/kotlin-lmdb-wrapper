package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.customarrays

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.VarSizeRWP
import java.nio.ByteBuffer

abstract class CustomArrayRWP<M: BaseLMDBObject<M>, T>(private val isTNullable: Boolean, lmdbObject: BaseLMDBObject<M>, nullable: Boolean): VarSizeRWP<M, Array<T>?>(lmdbObject, nullable) {

}