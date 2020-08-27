package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import kotlin.Comparator
import kotlin.collections.HashMap
import kotlin.reflect.KProperty1
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.jvm.isAccessible

/**
 * A basic LMDBObject that is of the class [DbiType] which extends [LMDBObject].
 *
 * @constructor
 * Initialize the buffers and update the offsets
 *
 * @param[from] The way to create this object
 */
abstract class LMDBObject<DbiType : LMDBObject<DbiType>>(private val dbi: LMDBDbi<DbiType>, from: BufferType) {
    private val propMap: TreeMap<Triple<String, Boolean, Boolean>, KProperty1<DbiType, *>>? = if (from == BufferType.DbiObject) TreeMap(pairComparator) else null
    private val rwpsMap = TreeMap<Triple<String, Boolean, Boolean>, AbstractRWP<DbiType, *>>(pairComparator)

    internal lateinit var constSizeMap: Map<KProperty1<DbiType, *>, Triple<Int, Boolean, RWPCompanion<*, *>>> //Name of const size items to their positions

    private lateinit var rwpsOrdered: Array<AbstractRWP<DbiType, *>>
    internal lateinit var nullables: BooleanArray

    /**
     * Returns the size of this object in-DB
     */
    val size: Int
        get() {
            setUsed()
            return rwpsOrdered.map { it.getDiskSize(dbi.nullStoreOption) }.sum()
        }

    /**
     * True if this object has been committed to the DB, or read from the DB and not modified.
     */
    var committed: Boolean
        private set
    private var firstBuf: ByteBuffer?
    protected var isInit = false
        private set

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
     * Adds the [property] that is backed by [rwp].
     */
    @PublishedApi
    internal fun addType(property: KProperty1<DbiType, *>, rwp: AbstractRWP<DbiType, *>) {
        require(!isInit) { "Cannot add new DB items after first access!" }
        val key = Triple(property.name, property.returnType.isMarkedNullable, rwp is ConstSizeRWP<*, *>)
        require(!(propMap?.containsKey(key) ?: false)) { "Cannot have the same name twice!" }
        propMap?.set(key, property)
        rwpsMap[key] = rwp
    }

    private fun setTypes() {
        require(rwpsMap.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        this.rwpsOrdered = rwpsMap.values.toTypedArray()
        if (committed && firstBuf == null) {
            this.nullables = BooleanArray(propMap!!.size)
            val tNameMap = HashMap<String, Int>()
            val tempIter = rwpsMap.iterator()
            var offset = 0
            val writeMap = HashMap<KProperty1<DbiType, *>, Triple<Int, Boolean, RWPCompanion<*, *>>>()
            propMap.entries.forEachIndexed { index, s ->
                nullables[index] = s.value.returnType.isMarkedNullable
                tNameMap[s.key.first] = index
                if (s.key.third /*const size*/ &&
                    (!s.key.second /*not-null*/ || (s.key.second && dbi.nullStoreOption == NullStoreOption.SPEED) /*nullable and speed option*/)) {
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
        } else {
            this.nullables = dbi.nullables
        }
    }

    /**
     * Initialize the value of the object backed by [kProperty] to [item].
     */
    protected fun <T> set(kProperty: KProperty1<DbiType, T>, item: T) {
        require(!isInit) { "Do not allow changing of items after initialization!" }
        val oldAccessible = kProperty.isAccessible
        kProperty.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        (kProperty.getDelegate(this as DbiType) as AbstractRWP<DbiType, T>).setValue(this, kProperty, item)
        kProperty.isAccessible = oldAccessible
    }

    /**
     * Wrapper for assigning objects based on type and annotation
     */
    protected val db by lazy { RWPProvider(this) }

    /**
     * Returns the key size for this object. Defaults to 8, size of a [Long] on a 64-bit platform.
     */
    open fun keySize(): Int = 8

    /**
     * Returns a key that fits in the [keyBuffer]
     */
    abstract fun keyFunc(keyBuffer: ByteBuffer)

    internal fun writeToBuffer(buffer: ByteBuffer) {
        var off = 0
        for (i in rwpsOrdered) {
            off += i.writeToDB(buffer, off, dbi.nullStoreOption)
        }
    }

    /**
     * Writes only this object into the [dbi] of [env].
     * The key will be the return of [keyFunc].
     */
    fun write() {
        if (!isInit) {
            setUsed()
        }
        require(dbi.handle != -1) { "DBI must be initialized to store objects!" }
        dbi.env.getOrCreateWriteTx { stack, tx ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.mallocStack(stack).mv_data(key)

            val dv = MDBVal.mallocStack(stack).mv_size(size.toLong())

            try {
                LMDB_CHECK(mdb_put(tx.tx, dbi.handle, kv, dv, MDB_RESERVE))

                writeToBuffer(dv.mv_data()!!)
            } catch (t: Throwable) {
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
    fun read() {
        if (!isInit) {
            setUsed()
        }
        require(dbi.handle != -1) { "DBI must be initialized to read objects!" }
        dbi.env.getOrCreateReadTx { stack, readTx ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val dv = MDBVal.callocStack()
            val err = mdb_get(readTx.tx, dbi.handle, kv, dv)
            if (err == MDB_NOTFOUND) {
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                try {
                    LMDB_CHECK(err)
                } catch (t: Throwable) {
                    throw t
                }
            }

            initBuffers(dv.mv_data()!!)
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
        dbi.env.getOrCreateWriteTx { stack, tx ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val data = if ((dbi.flags and MDB_DUPSORT) == MDB_DUPSORT) {
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

            val err = mdb_del(tx.tx, dbi.handle, kv, data)
            if (err == MDB_NOTFOUND) {
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                try {
                    LMDB_CHECK(err)
                } catch (t: Throwable) {
                    throw t
                }
            }
        }
    }

    /**
     * Helper functions applicable to all [LMDBObject] types
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
    }
}