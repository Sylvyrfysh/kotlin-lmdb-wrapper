package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.*
import kotlin.reflect.KClassifier
import kotlin.reflect.KFunction1
import kotlin.reflect.KMutableProperty0
import kotlin.reflect.KProperty

/**
 * Returns objects for the class of type [M], instance [obj].
 *
 * @constructor
 * Takes in the class instance [obj] to provide for.
 */
class LMDBBaseObjectProvider<M: BaseLMDBObject<M>>(@PublishedApi internal val obj: BaseLMDBObject<M>) {
    /**
     * Provides a delegate for the class instance [thisRef]'s property [prop].
     * Returns a RWPInterface with the right getters and setters.
     */
    operator fun provideDelegate(thisRef: M?, prop: KProperty<*>): RWPInterface<M> {
        return getTypeDelegate(prop.returnType.classifier, prop)
    }

    @PublishedApi
    internal fun getTypeDelegate(type: KClassifier?, prop: KProperty<*>): RWPInterface<M> {
        val rwp =  when (type) {
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
            ByteArray::class -> {
                TODO("ArraySize annotations")
            }
            else -> error("New type no impl ${prop.returnType}")
        }
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }

    inline fun <reified D, reified R> custom(
        fromDBToObj: KFunction1<D, R>,
        fromObjToDB: KFunction1<R, D>,
        prop: KMutableProperty0<R>
    ): RWPInterface<M> {
        val d1 = getTypeDelegate(D::class, prop)
        @Suppress("UNCHECKED_CAST")
        return TypeWrapperRWP(d1 as AbstractRWP<M, D?>, fromDBToObj, fromObjToDB, obj, prop.name)
    }
}
