package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import java.lang.RuntimeException
import kotlin.reflect.KProperty

open class LMDBBaseObjectProvider<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>) {
    operator fun provideDelegate(thisRef: M, prop: KProperty<*>): SpecialRWP<M> {
        println(prop.returnType.classifier == Byte::class.java.kotlin)
        return when (prop.returnType.classifier) {
            Boolean::class -> BoolRWP(obj.getInd(prop.name))
            Byte::class -> ByteRWP(obj.getInd(prop.name))
            Short::class -> ShortRWP(obj.getInd(prop.name))
            Char::class -> CharRWP(obj.getInd(prop.name))
            Int::class -> IntRWP(obj.getInd(prop.name))
            Float::class -> FloatRWP(obj.getInd(prop.name))
            Long::class -> {
                return if (prop.annotations.filterIsInstance<VarLong>().isNotEmpty()) {
                    TODO("VarLong is not yet implemented!")
                } else {
                    LongRWP(obj.getInd(prop.name))
                }
            }
            Double::class -> DoubleRWP(obj.getInd(prop.name))
            String::class -> {
                val maxL = (prop.annotations.filterIsInstance<VarChar>().firstOrNull() ?: error("Strings must have the VarChar annotation")).maxLength
                require(maxL > 0) { "VarChar must have a length of at least one!" }
                println(maxL)
                TODO("VarChar is not yet implemented!")
            }
            else -> error("New type no impl ${prop.returnType}")
        }
    }
}
