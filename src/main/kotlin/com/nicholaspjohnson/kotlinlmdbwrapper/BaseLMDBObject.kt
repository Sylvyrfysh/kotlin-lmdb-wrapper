package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import org.lwjgl.system.MemoryStack.stackPush
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*
import kotlin.collections.HashMap
import kotlin.reflect.KProperty0
import kotlin.reflect.jvm.isAccessible

/**
 * A basic LMDBObject that is of the class [M] which extends [BaseLMDBObject].
 *
 * @constructor
 * Initialize the buffers and update the offsets
 *
 * @param[from] The way to create this object
 */
abstract class BaseLMDBObject<M : BaseLMDBObject<M>>(from: ObjectBufferType) {
    private val haveNullable = TreeMap<String, Boolean>()
    private val rwpsMap = TreeMap<String, AbstractRWP<M, *>>()

    private lateinit var nameToInts: Map<String, Int>

    private lateinit var rwpsOrdered: Array<AbstractRWP<M, *>>
    private lateinit var nullables: Array<Boolean>

    /**
     * Returns the size of this object in-DB
     */
    val size: Int
        get() = rwpsOrdered.map(AbstractRWP<M, *>::getDiskSize).sum()

    /**
     * True if this object has been committed to the DB, or read from the DB and not modified.
     */
    var committed: Boolean
        private set
    private var firstBuf: ByteBuffer?
    private var isInit = false

    init {
        when (from) {
            is ObjectBufferType.None -> {
                firstBuf = null
                committed = false
            }
            is ObjectBufferType.Buffer -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
                committed = false
            }
            is ObjectBufferType.DBRead -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
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
            }
            firstBuf = null
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
            off += i.readFromDB(newData, off)
        }
    }

    /**
     * Adds an object to this object with the name [name] that is [nullable], backed by [rwp].
     */
    fun addType(name: String, rwp: AbstractRWP<M, *>, nullable: Boolean) {
        require(!haveNullable.containsKey(name)) { "Cannot have the same name twice!" }
        require(!isInit) { "Cannot add new DB items after first access!" }
        haveNullable[name] = nullable
        rwpsMap[name] = rwp
    }

    private fun setTypes() {
        require(haveNullable.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        val nullableIter = haveNullable.entries.iterator()
        this.nullables = Array(haveNullable.size) {
            nullableIter.next().value
        }
        val tNameMap = HashMap<String, Int>()
        haveNullable.keys.forEachIndexed { index, s ->
            tNameMap[s] = index
        }
        this.nameToInts = tNameMap
        val rwpOrderedIter = rwpsMap.entries.iterator()
        this.rwpsOrdered = Array(haveNullable.size) {
            rwpOrderedIter.next().value
        }
    }

    /**
     * Initialize the value of the object backed by [kProperty] to [item].
     */
    protected fun <T> set(kProperty: KProperty0<T>, item: T) {
        val oldAccessible = kProperty.isAccessible
        kProperty.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        (kProperty.getDelegate() as AbstractRWP<M, T>).setValue(this, kProperty, item)
        kProperty.isAccessible = oldAccessible
    }

    /**
     * Wrapper for assigning objects based on type and annotation
     */
    protected val db by lazy { LMDBBaseObjectProvider(this) }

    /**
     * Returns the key size for this object. Defaults to 8.
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
    fun writeInSingleTX(env: Long, dbi: Int) {
        if (!isInit) {
            setUsed()
        }
        stackPush().use { stack ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val dv = MDBVal.callocStack(stack).mv_size(size.toLong())

            val pp = stack.mallocPointer(1)

            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            try {
                LMDB_CHECK(mdb_put(txn, dbi, kv, dv, MDB_RESERVE))

                val writeDB = dv.mv_data()!!
                var off = 0
                for (i in rwpsOrdered) {
                    off += i.writeToDB(writeDB, off)
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
    fun readFromDB(env: Long, dbi: Int) {
        if (!isInit) {
            setUsed()
        }
        stackPush().use { stack ->
            val key = stack.malloc(keySize())
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val pp = stack.mallocPointer(1)
            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
            val txn = pp.get(0)

            val dv = MDBVal.callocStack()
            val err = mdb_get(txn, dbi, kv, dv)
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

    companion object {

    }
}