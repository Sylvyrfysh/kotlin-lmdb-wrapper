package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list.ListRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.*
import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KFunction1
import kotlin.reflect.KProperty
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance

/**
 * Returns objects for the class of type [M], instance [obj].
 *
 * @constructor
 * Takes in the class instance [obj] to provide for.
 */
@Suppress("UNCHECKED_CAST")
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
        val nullable = prop.returnType.isMarkedNullable
        val rwpClass =  getRWPClass(type, prop.annotations)
        val rwp = rwpClass.constructors.first().call(obj, nullable)
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }

    @PublishedApi
    internal fun getRWPClass(type: KClassifier?, annotations: List<Annotation>?): KClass<AbstractRWP<M, *>> = when (type) {
        Boolean::class -> BoolRWP::class
        Byte::class -> ByteRWP::class
        Short::class -> ShortRWP::class
        Char::class -> CharRWP::class
        Int::class -> IntRWP::class
        Float::class -> FloatRWP::class
        Long::class -> {
            if (annotations?.filterIsInstance<VarLong>()?.isNotEmpty() == true) {
                VarLongRWP::class
            } else {
                LongRWP::class
            }
        }
        Double::class -> DoubleRWP::class
        String::class -> {
            VarCharRWP::class
        }
        BooleanArray::class -> BoolArrayRWP::class
        ByteArray::class -> ByteArrayRWP::class
        CharArray::class -> CharArrayRWP::class
        DoubleArray::class -> DoubleArrayRWP::class
        FloatArray::class -> FloatArrayRWP::class
        IntArray::class -> IntArrayRWP::class
        LongArray::class -> LongArrayRWP::class
        ShortArray::class -> ShortArrayRWP::class
        List::class -> error("Use db.list!")
        else -> error("New type no impl $type")
    } as KClass<AbstractRWP<M, *>>

    inline fun <reified DBType, reified ObjectType> custom(
        fromDBToObj: KFunction1<DBType, ObjectType>,
        fromObjToDB: KFunction1<ObjectType, DBType>,
        prop: KProperty<ObjectType>
    ): AbstractRWP<M, ObjectType> {
        val d1 = getTypeDelegate(DBType::class, prop)
        @Suppress("UNCHECKED_CAST")
        return TypeWrapperRWP(d1 as AbstractRWP<M, DBType?>, fromDBToObj, fromObjToDB, obj, prop.returnType.isMarkedNullable)
    }

    inline fun <reified ItemType, reified ListType: List<ItemType>> list(
        prop: KProperty<ListType>,
        noinline newListFn: () -> MutableList<ItemType>
    ): ListRWP<M, ItemType, List<ItemType>> {
        val underlyingCompanion = getRWPClass(ItemType::class, null).companionObjectInstance as RWPCompanion<AbstractRWP<*, ItemType>, ItemType>
        val rwp = ListRWP(newListFn, underlyingCompanion, obj, prop.returnType.isMarkedNullable)
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }
}
