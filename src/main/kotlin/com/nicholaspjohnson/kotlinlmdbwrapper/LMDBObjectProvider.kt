package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import kotlin.reflect.KProperty

/**
 * Returns objects for the class of type [M], instance [obj].
 *
 * @constructor
 * Takes in the class instance [obj] to provide for.
 */
class LMDBBaseObjectProvider<M: BaseLMDBObject<M>>(private val obj: BaseLMDBObject<M>) {
    /**
     * Provides a delegate for the class instance [thisRef]'s property [prop].
     * Returns a RWPInterface with the right getters and setters.
     */
    operator fun provideDelegate(thisRef: M, prop: KProperty<*>): RWPInterface<M> {
        val rwp =  when (prop.returnType.classifier) {
            Boolean::class -> BoolRWP(obj, prop.name)
            Byte::class -> ByteRWP(obj, prop.name)
            Short::class -> ShortRWP(obj, prop.name)
            Char::class -> CharRWP(obj, prop.name)
            Int::class -> IntRWP(obj, prop.name)
            Float::class -> FloatRWP(obj, prop.name)
            Long::class -> {
                if (prop.annotations.filterIsInstance<VarLong>().isNotEmpty()) {
                    VarLongRWP(obj, prop.name)
                } else {
                    LongRWP(obj, prop.name)
                }
            }
            Double::class -> DoubleRWP(obj, prop.name)
            String::class -> {
                val maxL = (prop.annotations.filterIsInstance<VarChar>().firstOrNull() ?: error("Strings must have the VarChar annotation")).maxLength
                VarCharRWP(obj, prop.name, maxL)
            }
            else -> error("New type no impl ${prop.returnType}")
        }
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }
}
