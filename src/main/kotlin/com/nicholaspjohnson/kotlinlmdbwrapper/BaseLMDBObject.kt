package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import org.lwjgl.system.MemoryStack.stackPush
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import kotlin.Comparator
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.reflect.KProperty1
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.jvm.isAccessible

/**
 * A basic LMDBObject that is of the class [M] which extends [BaseLMDBObject].
 *
 * @constructor
 * Initialize the buffers and update the offsets
 *
 * @param[from] The way to create this object
 */
abstract class BaseLMDBObject<M : BaseLMDBObject<M>>(private val dbi: LMDBDbi<M>, from: BufferType) {
    private val propMap: TreeMap<Triple<String, Boolean, Boolean>, KProperty1<M, *>>? = if (from == BufferType.DbiObject) TreeMap(pairComparator) else null
    private val rwpsMap = TreeMap<Triple<String, Boolean, Boolean>, AbstractRWP<M, *>>(pairComparator)

    internal lateinit var nameToIndices: Map<String, Int> //Name to index of position
    internal lateinit var constSizeMap: Map<KProperty1<M, *>, Triple<Int, Boolean, RWPCompanion<*, *>>> //Name of const size items to their positions

    private lateinit var rwpsOrdered: Array<AbstractRWP<M, *>>
    internal lateinit var nullables: Array<Boolean>

    /**
     * Returns the size of this object in-DB
     */
    val size: Int
        get() = rwpsOrdered.map{ it.getDiskSize(dbi.nullStoreOption) }.sum()

    /**
     * True if this object has been committed to the DB, or read from the DB and not modified.
     */
    var committed: Boolean
        private set
    private var firstBuf: ByteBuffer?
    private var isInit = false

    init {
        when (from) {
            is BufferType.None -> {
                firstBuf = null
                committed = false
            }
            is BufferType.Buffer -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
                committed = false
            }
            is BufferType.DBRead -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
                committed = true
            }
            is BufferType.DbiObject -> {
                firstBuf = null
                committed = true
            }
        }
    }

    /**
     * If this object has not yet been used, initialize the buffer and set the initialized flag.
     */
    internal fun setUsed() {
        if (!isInit) {
            setTypes()
            if (firstBuf != null) {
                initBuffers(firstBuf!!)
                firstBuf = null
            }
            isInit = true
        }
    }

    private fun checkBuffer(buffer: ByteBuffer) {
        require(buffer.order() == ByteOrder.nativeOrder()) { "The buffer order must be equal to ByteOrder.nativeOrder()!" }
    }

    /**
     * Reads and sets the data and will redo offsets if read from DB
     */
    private fun initBuffers(newData: ByteBuffer) {
        checkBuffer(newData)

        var off = 0
        for (i in rwpsOrdered) {
            off += i.readFromDB(newData, off, dbi.nullStoreOption)
        }
    }

    /**
     * Adds an object to this object with the name [name] that is [nullable], backed by [rwp].
     */
    fun addType(prop: KProperty1<M, *>, rwp: AbstractRWP<M, *>) {
        require(!isInit) { "Cannot add new DB items after first access!" }
        val key = Triple(prop.name, prop.returnType.isMarkedNullable, rwp is ConstSizeRWP<*, *>)
        require(!(propMap?.containsKey(key) ?: false)) { "Cannot have the same name twice!" }
        propMap?.set(key, prop)
        rwpsMap[key] = rwp
    }

    private fun setTypes() {
        require(rwpsMap.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        val rwpOrderedIter = rwpsMap.entries.iterator()
        this.rwpsOrdered = Array(rwpsMap.size) {
            rwpOrderedIter.next().value
        }
        if (committed && firstBuf == null) {
            this.nullables = Array(propMap!!.size) { false }
            val tNameMap = HashMap<String, Int>()
            val tempIter = rwpsMap.iterator()
            var offset = 0
            val writeMap = HashMap<KProperty1<M, *>, Triple<Int, Boolean, RWPCompanion<*, *>>>()
            propMap.entries.forEachIndexed { index, s ->
                nullables[index] = s.value.returnType.isMarkedNullable
                tNameMap[s.key.first] = index
                if (s.key.third /*const size*/ && (!s.key.second /*not-null*/ || (s.key.second && dbi.nullStoreOption == NullStoreOption.SPEED) /*nullable and speed option*/)) {
                    val tin = tempIter.next()
                    if (tin.value::class.companionObjectInstance is RWPCompanion<*, *>) {
                        writeMap[s.value] =
                            Triple(
                                offset,
                                nullables[index],
                                tin.value::class.companionObjectInstance as RWPCompanion<*, *>
                            )
                    }
                    offset += tin.value.getDiskSize(dbi.nullStoreOption)
                }
            }
            constSizeMap = Collections.unmodifiableMap(writeMap)
            this.nameToIndices = tNameMap
        } else {
            this.nameToIndices = dbi.nameToIndices
            this.nullables = dbi.nullables
        }
    }

    /**
     * Initialize the value of the object backed by [kProperty] to [item].
     */
    protected fun <T> set(kProperty: KProperty1<M, T>, item: T) {
        require(!isInit) { "Do not allow changing of items after initialization!" }
        val oldAccessible = kProperty.isAccessible
        kProperty.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        (kProperty.getDelegate(this as M) as AbstractRWP<M, T>).setValue(this, kProperty, item)
        kProperty.isAccessible = oldAccessible
    }

    /**
     * Wrapper for assigning objects based on type and annotation
     */
    protected val db by lazy { LMDBBaseObjectProvider(this) }

    /**
     * Returns the key size for this object. Defaults to 8, size of a [Long] on a 64-bit platform.
     */
    open fun keySize(): Int = 8

    /**
     * Returns a key that fits in the [keyBuffer]
     */
    protected abstract fun keyFunc(keyBuffer: ByteBuffer)

    /**
     * Writes only this object into the [dbi] of [env].
     * The key will be the return of [keyFunc].
     */
    fun writeInSingleTX() {
        if (!isInit) {
            setUsed()
        }
        require(dbi.handle != -1) { "DBI must be initialized to store objects!" }
        stackPush().use { stack ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val dv = MDBVal.callocStack(stack).mv_size(size.toLong())

            val pp = stack.mallocPointer(1)

            LMDB_CHECK(mdb_txn_begin(dbi.env.handle, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            try {
                LMDB_CHECK(mdb_put(txn, dbi.handle, kv, dv, MDB_RESERVE))

                val writeDB = dv.mv_data()!!
                var off = 0
                for (i in rwpsOrdered) {
                    off += i.writeToDB(writeDB, off, dbi.nullStoreOption)
                }

                LMDB_CHECK(mdb_txn_commit(txn))
            } catch (t: Throwable) {
                mdb_txn_abort(txn)
                throw t
            }

            committed = true
        }
    }

    /**
     * Based on the key provided by [keyFunc], attempts to load all of the other data members.
     * If the key does not exist, a [DataNotFoundException] will be thrown.
     */
    @Throws(DataNotFoundException::class)
    fun readFromDB() {
        if (!isInit) {
            setUsed()
        }
        require(dbi.handle != -1) { "DBI must be initialized to read objects!" }
        stackPush().use { stack ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val pp = stack.mallocPointer(1)
            LMDB_CHECK(mdb_txn_begin(dbi.env.handle, MemoryUtil.NULL, MDB_RDONLY, pp))
            val txn = pp.get(0)

            val dv = MDBVal.callocStack()
            val err = mdb_get(txn, dbi.handle, kv, dv)
            if (err == MDB_NOTFOUND) {
                mdb_txn_abort(txn)
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                try {
                    LMDB_CHECK(err)
                } catch (t: Throwable) {
                    mdb_txn_abort(txn)
                    throw t
                }
            }

            initBuffers(dv.mv_data()!!)
            mdb_txn_abort(txn)
        }
    }

    /**
     * Based on the key provided by [keyFunc], attempts to delete this item.
     * If the key does not exist, a [DataNotFoundException] will be thrown.
     */
    fun delete() {
        if (!isInit) {
            setUsed()
        }
        require(dbi.handle != -1) { "DBI must be initialized to delete objects!" }
        stackPush().use { stack ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val pp = stack.mallocPointer(1)
            LMDB_CHECK(mdb_txn_begin(dbi.env.handle, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            val flags = stack.mallocInt(1)
            LMDB_CHECK(mdb_dbi_flags(txn, dbi.handle, flags))

            val data = if ((flags.get(0) and MDB_DUPSORT) == MDB_DUPSORT) {
                val dv = MDBVal.callocStack(stack).mv_data(stack.malloc(size))
                dv.mv_size(size.toLong())
                var off = 0
                for (i in rwpsOrdered) {
                    off += i.writeToDB(dv.mv_data()!!, off, dbi.nullStoreOption)
                }
                dv
            } else {
                null
            }

            val err = mdb_del(txn, dbi.handle, kv, data)
            if (err == MDB_NOTFOUND) {
                mdb_txn_abort(txn)
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                try {
                    LMDB_CHECK(err)

                    LMDB_CHECK(mdb_txn_commit(txn))
                } catch (t: Throwable) {
                    mdb_txn_abort(txn)
                    throw t
                }
            }
        }
    }

    /**
     * Helper functions applicable to all [BaseLMDBObject] types
     */
    companion object {
        private val pairComparator = Comparator<Triple<String, Boolean, Boolean>> { o1, o2 ->
            return@Comparator when {
                o1.third == o2.third -> when {
                    o1.second == o2.second -> o1.first.compareTo(o2.first)
                    o2.second -> -1
                    else -> 1
                }
                o1.third -> -1
                else -> 1
            }
        }

        /**
         * Returns true if any item in the [dbi] of [env] has a value of [item] for [property].
         */
        inline fun <reified M : BaseLMDBObject<M>, T> hasObjectWithValue(
            env: Long,
            dbi: Int,
            property: KProperty1<M, T>,
            item: T
        ): Boolean = getObjectsWithValue(env, dbi, property, item).isNotEmpty()

        /**
         * Returns all items in the [dbi] of [env] with a value of [item] for [property]. If none match, an empty list is returned.
         */
        inline fun <reified M : BaseLMDBObject<M>, T> getObjectsWithValue(
            env: Long,
            dbi: Int,
            property: KProperty1<M, T>,
            item: T
        ): List<M> {
            val const =
                M::class.constructors.first { it.parameters.size == 1 && it.parameters.first().type.classifier == BufferType::class }

            val ret = ArrayList<M>()
            stackPush().use { stack ->
                val pp = stack.mallocPointer(1)

                LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
                val txn = pp.get(0)

                LMDB_CHECK(mdb_cursor_open(txn, dbi, pp.position(0)))
                val cursor = pp.get(0)

                val data = MDBVal.mallocStack(stack)
                val key = MDBVal.mallocStack(stack)

                var rc = mdb_cursor_get(cursor, key, data, MDB_FIRST)

                while (rc != MDB_NOTFOUND) {
                    LMDB_CHECK(rc)
                    val obj = const.call(BufferType.DBRead(data.mv_data()!!))
                    if (property.get(obj)?.equals(item) == true) {
                        ret.add(obj)
                    }
                    rc = mdb_cursor_get(cursor, key, data, MDB_NEXT)
                }

                mdb_cursor_close(cursor)
                mdb_txn_abort(txn)
                return ret
            }
        }

        /**
         * Returns true if any item in the [dbi] of [env] has a value of [item] for [property] with the equality function [equalityFunc].
         */
        inline fun <reified M : BaseLMDBObject<M>, T, R> hasObjectWithEquality(
            env: Long,
            dbi: Int,
            property: KProperty1<M, T>,
            item: R,
            equalityFunc: (T, R) -> Boolean
        ): Boolean = getObjectsWithEquality(env, dbi, property, item, equalityFunc).isNotEmpty()

        /**
         * Returns all items in the [dbi] of [env] with a value of [item] for [property] with [equalityFunc]. If none match, an empty list is returned.
         */
        inline fun <reified M : BaseLMDBObject<M>, T, R> getObjectsWithEquality(
            env: Long,
            dbi: Int,
            property: KProperty1<M, T>,
            item: R,
            equalityFunc: (T, R) -> Boolean
        ): List<M> {
            val const =
                M::class.constructors.first { it.parameters.size == 1 && it.parameters.first().type.classifier == BufferType::class }

            val ret = ArrayList<M>()
            stackPush().use { stack ->
                val pp = stack.mallocPointer(1)

                LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
                val txn = pp.get(0)

                LMDB_CHECK(mdb_cursor_open(txn, dbi, pp.position(0)))
                val cursor = pp.get(0)

                val data = MDBVal.mallocStack(stack)
                val key = MDBVal.mallocStack(stack)

                var rc = mdb_cursor_get(cursor, key, data, MDB_FIRST)

                while (rc != MDB_NOTFOUND) {
                    LMDB_CHECK(rc)
                    val obj = const.call(BufferType.DBRead(data.mv_data()!!))
                    if (equalityFunc(property.get(obj)!!, item)) {
                        ret.add(obj)
                    }
                    rc = mdb_cursor_get(cursor, key, data, MDB_NEXT)
                }

                mdb_cursor_close(cursor)
                mdb_txn_abort(txn)
                return ret
            }
        }

        /**
         * Iterates over the [dbi] of [env] and passes all items to [block].
         */
        inline fun <reified M : BaseLMDBObject<M>> forEach(env: Long, dbi: Int, block: (M) -> Unit) {
            val const =
                M::class.constructors.first { it.parameters.size == 1 && it.parameters.first().type.classifier == BufferType::class }
            stackPush().use { stack ->
                val pp = stack.mallocPointer(1)

                LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
                val txn = pp.get(0)

                LMDB_CHECK(mdb_cursor_open(txn, dbi, pp.position(0)))
                val cursor = pp.get(0)

                val data = MDBVal.mallocStack(stack)
                val key = MDBVal.mallocStack(stack)

                var rc = mdb_cursor_get(cursor, key, data, MDB_FIRST)

                while (rc != MDB_NOTFOUND) {
                    LMDB_CHECK(rc)
                    block(const.call(BufferType.DBRead(data.mv_data()!!)))
                    rc = mdb_cursor_get(cursor, key, data, MDB_NEXT)
                }

                mdb_cursor_close(cursor)
                mdb_txn_abort(txn)
            }
        }
    }
}