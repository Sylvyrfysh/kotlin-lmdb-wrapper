package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import kotlin.reflect.KProperty

open class LMDBBaseObjectProvider<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>) {
    operator fun provideDelegate(thisRef: M, prop: KProperty<*>): SpecialRWP<M> {
        return when (prop.returnType.classifier) {
            Boolean::class -> BoolRWP(obj, prop.name)
            Byte::class -> ByteRWP(obj, prop.name)
            Short::class -> ShortRWP(obj, prop.name)
            Char::class -> CharRWP(obj, prop.name)
            Int::class -> IntRWP(obj, prop.name)
            Float::class -> FloatRWP(obj, prop.name)
            Long::class -> {
                return if (prop.annotations.filterIsInstance<VarLong>().isNotEmpty()) {
                    TODO("VarLong is not yet implemented!")
                } else {
                    LongRWP(obj, prop.name)
                }
            }
            Double::class -> DoubleRWP(obj, prop.name)
            String::class -> {
                val maxL = (prop.annotations.filterIsInstance<VarChar>().firstOrNull() ?: error("Strings must have the VarChar annotation")).maxLength
                require(maxL > 0) { "VarChar must have a length of at least one!" }
                TODO("VarChar is not yet implemented!")
            }
            else -> error("New type no impl ${prop.returnType}")
        }
    }
}
