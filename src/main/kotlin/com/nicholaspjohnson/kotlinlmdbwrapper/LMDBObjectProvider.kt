package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.arrays.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.*
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.list.CollectionRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.map.MapRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize.*
import kotlin.reflect.KClass
import kotlin.reflect.KClassifier
import kotlin.reflect.KFunction1
import kotlin.reflect.KProperty
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

    /**
     * Returns an [RWPInterface] that is for [type] on the object [prop], and registers it with the [BaseLMDBObject].
     */
    @PublishedApi
    internal fun getTypeDelegate(type: KClassifier?, prop: KProperty<*>): RWPInterface<M> {
        val nullable = prop.returnType.isMarkedNullable
        val rwpClass =  getRWPClass(type, prop.annotations)
        val rwp = rwpClass.constructors.first().call(obj, nullable)
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }

    /**
     * Returns a [KClass] of an [AbstractRWP] that is for [type] and has [annotations].
     */
    @PublishedApi
    internal fun getRWPClass(type: KClassifier?, annotations: List<Annotation>?): KClass<AbstractRWP<M, *>> {
        require(type != null) { "The given type must not be null!" }
        return when (type) {
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
            String::class -> VarCharRWP::class
            BooleanArray::class -> BoolArrayRWP::class
            ByteArray::class -> ByteArrayRWP::class
            CharArray::class -> CharArrayRWP::class
            DoubleArray::class -> DoubleArrayRWP::class
            FloatArray::class -> FloatArrayRWP::class
            IntArray::class -> IntArrayRWP::class
            LongArray::class -> LongArrayRWP::class
            ShortArray::class -> ShortArrayRWP::class
            in extraRWPS -> extraRWPS.getValue(type)
            Collection::class -> error("Use db.collection!")
            Map::class -> error("Use db.map!")
            else -> error("There is no RWP for the type $type!")
        } as KClass<AbstractRWP<M, *>>
    }

    /**
     * Returns a wrapped [AbstractRWP] for [prop] that is written on database as [DBType] and in code as [ObjectType].
     * Uses [fromDBToObj] and [fromObjToDB] to convert between these two states.
     */
    inline fun <reified DBType, reified ObjectType> custom(
        fromDBToObj: KFunction1<DBType, ObjectType>,
        fromObjToDB: KFunction1<ObjectType, DBType>,
        prop: KProperty<ObjectType>
    ): AbstractRWP<M, ObjectType> {
        val d1 = getTypeDelegate(DBType::class, prop)
        @Suppress("UNCHECKED_CAST")
        return TypeWrapperRWP(d1 as AbstractRWP<M, DBType?>, fromDBToObj, fromObjToDB, obj, prop.returnType.isMarkedNullable)
    }

    /**
     * Returns a [CollectionRWP] for [prop] that is for the collection type [CollectionType] that holds [ItemType].
     * Uses [newListFn] to create anew mutable version of [CollectionType] that will have items added on DB read that will then be assigned to the backing field.
     */
    inline fun <reified ItemType, reified CollectionType: List<ItemType>> collection(
        prop: KProperty<CollectionType>,
        noinline newListFn: () -> MutableCollection<ItemType>
    ): CollectionRWP<M, ItemType, Collection<ItemType>> {
        val underlyingCompanionObj = getRWPClass(ItemType::class, null).companionObjectInstance
        checkNotNull(underlyingCompanionObj) { "The underlying class does not have a companion object the list can work through!" }
        check(underlyingCompanionObj is RWPCompanion<*, *>) { "Companion object for item class ${ItemType::class.simpleName} does not extend RWPCompanion!" }
        check(!isNullable<ItemType>()) { "Null item types are not yet supported!" }
        val underlyingCompanion = underlyingCompanionObj as RWPCompanion<AbstractRWP<*, ItemType>, ItemType>
        val rwp = CollectionRWP(newListFn, underlyingCompanion, obj, prop.returnType.isMarkedNullable)
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }

    /**
     * Returns a [MapRWP] for [prop] that is for the map type [MapType] that is keyed by [KeyType] and valued by [DataType].
     * Uses [newMapFn] to create anew mutable version of [MapType] that will have items added on DB read that will then be assigned to the backing field.
     */
    inline fun <reified KeyType, reified DataType, reified MapType: Map<KeyType, DataType>> map(
        prop: KProperty<MapType>,
        noinline newMapFn: () -> MutableMap<KeyType, DataType>
    ): MapRWP<M, KeyType, DataType, Map<KeyType, DataType>> {
        val underlyingKeyCompanionObj = getRWPClass(KeyType::class, null).companionObjectInstance
        checkNotNull(underlyingKeyCompanionObj) { "The underlying key class does not have a companion object the map can work through!" }
        check(underlyingKeyCompanionObj is RWPCompanion<*, *>) { "Companion object for key class ${KeyType::class.simpleName} does not extend RWPCompanion!" }
        check(!isNullable<KeyType>()) { "Null key types are not yet supported!" }
        val underlyingKeyCompanion = underlyingKeyCompanionObj as RWPCompanion<AbstractRWP<*, KeyType>, KeyType>

        val underlyingDataCompanionObj = getRWPClass(DataType::class, null).companionObjectInstance
        checkNotNull(underlyingDataCompanionObj) { "The underlying data class does not have a companion object the map can work through!" }
        check(underlyingDataCompanionObj is RWPCompanion<*, *>) { "Companion object for data class ${KeyType::class.simpleName} does not extend RWPCompanion!" }
        check(!isNullable<DataType>()) { "Null data types are not yet supported!" }
        val underlyingDataCompanion = underlyingDataCompanionObj as RWPCompanion<AbstractRWP<*, DataType>, DataType>

        val rwp = MapRWP(newMapFn, underlyingKeyCompanion, underlyingDataCompanion, obj, prop.returnType.isMarkedNullable)
        obj.addType(prop.name, rwp, prop.returnType.isMarkedNullable)
        return rwp
    }

    /**
     * Returns whether or not the given type [T] is nullable.
     */
    inline fun <reified T> isNullable() = null is T

    /**
     * Utilities to use related to user-specified RWPs
     */
    companion object {
        private val extraRWPS = HashMap<KClassifier?, KClass<out AbstractRWP<*, *>>>()

        /**
         * Adds a user-specified RWP of the class [rwpClass] for types of [typeFor].
         */
        fun addRWP(typeFor: KClassifier?, rwpClass: KClass<out AbstractRWP<*, *>>) {
            require(typeFor != null) { "The given type must not be null!" }
            extraRWPS[typeFor] = rwpClass
        }
    }
}
