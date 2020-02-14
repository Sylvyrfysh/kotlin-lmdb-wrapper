package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.BufferType
import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBStat
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import kotlin.reflect.KFunction1
import kotlin.reflect.KProperty1

open class LMDBDbi<T : LMDBObject<T>>(
    val name: String,
    private val constructor: KFunction1<BufferType, T>,
    internal val nullStoreOption: NullStoreOption = NullStoreOption.SPEED,
    private val flags: Int = 0
) {
    private lateinit var constOffsets: Map<KProperty1<T, *>, Triple<Int, Boolean, RWPCompanion<*, *>>>

    internal lateinit var nameToIndices: Map<String, Int> //Name to index of position
    internal lateinit var nullables: Array<Boolean>

    internal var handle: Int = -1
        private set
    internal lateinit var env: LMDBEnv

    private var isInit = false

    internal fun onLoadInternal(env: LMDBEnv) {
        require(!isInit) { "Cannot initialize an already initialized dbi!" }
        this.env = env
        preLoad()
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            val ip = stack.mallocInt(1)
            LMDB.mdb_txn_begin(env.handle, MemoryUtil.NULL, 0, pp)
            LMDB_CHECK(
                LMDB.mdb_dbi_open(pp[0], name, flags or LMDB.MDB_CREATE, ip)
            )
            handle = ip[0]
            LMDB.mdb_txn_commit(pp[0])
        }

        val obj = constructor(BufferType.DbiObject)
        obj.setUsed()
        constOffsets = obj.constSizeMap
        nameToIndices = obj.nameToIndices
        nullables = obj.nullables

        isInit = true
        postLoad()
    }

    /**
     * Functionality to call before the database is loaded.
     */
    open fun preLoad() {}

    /**
     * Functionality to call after the database is loaded.
     */
    open fun postLoad() {}

    /**
     * Closes this dbi.
     */
    internal fun onCloseInternal(lmdbEnv: LMDBEnv) {
        preClose()
        LMDB.mdb_dbi_close(lmdbEnv.handle, handle)
        isInit = false
        postClose()
    }

    /**
     * Functionality to call before the database is closed.
     */
    open fun preClose() {}

    /**
     * Functionality to call after the database is closed.
     */
    open fun postClose() {}

    private fun cursor(readOnly: Boolean = true, block: (Long, MDBVal, MDBVal) -> Unit) {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)

            LMDB_CHECK(LMDB.mdb_txn_begin(env.handle, MemoryUtil.NULL, if (readOnly) LMDB.MDB_RDONLY else 0, pp))
            val txn = pp.get(0)

            LMDB_CHECK(LMDB.mdb_cursor_open(txn, handle, pp.position(0)))
            val cursor = pp.get(0)

            val key = MDBVal.mallocStack(stack)
            val data = MDBVal.mallocStack(stack)

            block(cursor, key, data)

            LMDB.mdb_cursor_close(cursor)
            if (readOnly) {
                LMDB.mdb_txn_abort(txn)
            } else {
                LMDB_CHECK(LMDB.mdb_txn_commit(txn))
            }
        }
    }

    private fun cursorLoopKeyRange(lowKeyObject: T, highKeyObject: T, endInclusive: Boolean, block: (ByteBuffer) -> Unit) {
        val rangeLimit = if (endInclusive) 1 else 0
        cursor { cursor, key, data ->
            MemoryStack.stackPush().use { stack ->
                val txn = LMDB.mdb_cursor_txn(cursor)

                val lowBuffer = stack.malloc(lowKeyObject.keySize())
                lowKeyObject.keyFunc(lowBuffer)
                lowBuffer.position(0)
                val highBuffer = stack.malloc(highKeyObject.keySize())
                highKeyObject.keyFunc(highBuffer)
                highBuffer.position(0)

                val highKey = MDBVal.mallocStack(stack)
                highKey.mv_data(highBuffer)

                key.mv_data(lowBuffer)
                var rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_SET_RANGE)
                LMDB_CHECK(rc)
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_GET_CURRENT)

                while (rc != LMDB.MDB_NOTFOUND && LMDB.mdb_cmp(txn, handle, key, highKey) < rangeLimit) {
                    LMDB_CHECK(rc)
                    val buffer = data.mv_data()!!
                    block(buffer)
                    rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
                }
            }
        }
    }

    private fun cursorLoopFull(block: (ByteBuffer) -> Unit) {
        cursor { cursor, key, data ->
            var rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_FIRST)

            while (rc != LMDB.MDB_NOTFOUND) {
                LMDB_CHECK(rc)
                val buffer = data.mv_data()!!
                block(buffer)
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
            }
        }
    }

    fun forEach(block: (T) -> Unit) {
        cursorLoopFull {
            block(constructor(BufferType.DBRead(it)))
        }
    }

    fun getEach(): List<T> {
        val ret = ArrayList<T>()
        cursorLoopFull {
            ret += constructor(BufferType.DBRead(it))
        }
        return ret
    }

    fun <M> getElementsWithEquality(toCheck: Pair<KProperty1<T, M>, M>): List<T> =
        getElementsWithEquality(toCheck.first, toCheck.second)

    fun <M> getElementsWithEquality(prop: KProperty1<T, M>, value: M): List<T> {
        require(isInit)
        val (offset, nullable, companion) = constOffsets[prop] ?: Triple(0, false, null)
        val ret = ArrayList<T>()

        val fastPath: Boolean = prop in constOffsets
        val fastMethod = if (nullable) {
            ::checkKnownFastNullPath
        } else {
            ::checkKnownFastPath
        }

        if (fastPath) {
            cursorLoopFull { buffer ->
                if (fastMethod(companion!!, value, buffer, offset)) {
                    ret += constructor(BufferType.DBRead(buffer))
                }
            }
        } else {
            cursorLoopFull { buffer ->
                val item = constructor(BufferType.DBRead(buffer))
                if (checkKnownSlowPath(prop, value, item)) {
                    ret += item
                }
            }
        }

        return ret
    }

    fun <M> getElementsWithEqualityFunction(prop: KProperty1<T, M>, equalityFunction: (M) -> Boolean): List<T> {
        require(isInit)
        val (offset, _, companion) = constOffsets[prop] ?: Triple(0, false, null)
        val ret = ArrayList<T>()

        val fastPath: Boolean = prop in constOffsets

        @Suppress("UNCHECKED_CAST") val useFunc = equalityFunction as (Any?) -> Boolean

        if (fastPath) {
            cursorLoopFull { buffer ->
                if (checkEqualityFastPath(companion!!, useFunc, buffer, offset)) {
                    ret += constructor(BufferType.DBRead(buffer))
                }
            }
        } else {
            cursorLoopFull { buffer ->
                val item = constructor(BufferType.DBRead(buffer))
                if (checkEqualitySlowPath(prop, useFunc, item)) {
                    ret += item
                }
            }
        }

        return ret
    }

    fun getElementsWithEquality(vararg equalities: Pair<KProperty1<T, *>, Any?>): List<T> {
        val fastChecksList = equalities.filter { it.first in constOffsets }
        val fastChecks = Array(fastChecksList.size) {
            val details = constOffsets.getValue(fastChecksList[it].first)
            Triple(if (details.second) ::checkKnownFastNullPath else ::checkKnownFastPath, details, fastChecksList[it].second)
        }
        val slowChecks = equalities.filterNot { it.first in constOffsets }.toTypedArray()

        val ret = ArrayList<T>()

        cursorLoopFull { buffer ->
            var cont = true
            var index = 0
            while (cont && index < fastChecks.size) {
                val (fastCheck, info, value) = fastChecks[index++]
                cont = cont and fastCheck(info.third, value, buffer, info.first)
            }
            if (cont) {
                val item = constructor(BufferType.DBRead(buffer))
                index = 0
                while (cont && index < slowChecks.size) {
                    val (prop, value) = slowChecks[index++]
                    cont = cont and checkKnownSlowPath(prop, value, item)
                }
                if (cont) {
                    ret += item
                }
            }
        }
        return ret
    }

    fun getElementsByKeyRange(lowKeyObject: T, highKeyObject: T, endInclusive: Boolean = false): List<T> {
        val ret = ArrayList<T>()
        cursorLoopKeyRange(lowKeyObject, highKeyObject, endInclusive) {
            ret += constructor(BufferType.DBRead(it))
        }
        return ret
    }

    /**
     * Overload for [writeMultiple] with and [array].
     */
    fun writeMultiple(array: Array<T>) = writeMultiple(array.iterator())

    /**
     * Overload for [writeMultiple] with an [iterable].
     */
    fun writeMultiple(iterable: Iterable<T>) = writeMultiple(iterable.iterator())

    /**
     * Takes an [iterator], then iterates over it and writes each item to the database.
     */
    fun writeMultiple(iterator: Iterator<T>) {
        if (!iterator.hasNext()) {
            return
        }
        cursor(false) { cursor, key, data ->
            MemoryStack.stackPush().use { stack ->
                key.mv_data(stack.malloc(0))
                for (item in iterator) {
                    val itemKeySize = item.keySize()
                    if (itemKeySize.toLong() != key.mv_size()) {
                        key.mv_data(stack.malloc(itemKeySize))
                    }
                    item.keyFunc(key.mv_data()!!)
                    data.mv_size(item.size.toLong())
                    LMDB_CHECK(LMDB.mdb_cursor_put(cursor, key, data, LMDB.MDB_RESERVE))

                    item.writeToBuffer(data.mv_data()!!)
                }
            }
        }
    }

    /**
     * If this is called, [prop] is a non-const size property.
     * This means that we need to load the whole object to make sure we have the correct offsets.
     */
    private fun checkKnownSlowPath(prop: KProperty1<T, *>, value: Any?, item: T): Boolean {
        return prop.get(item) == value
    }

    /**
     * If this is called, [prop] is a non-const size property.
     * This means that we need to load the whole object to make sure we have the correct offsets.
     */
    private fun checkEqualitySlowPath(prop: KProperty1<T, *>, function: (Any?) -> Boolean, item: T): Boolean {
        return function(prop.get(item))
    }

    /**
     * If this is called, we have a speed-optimized database with a potentially null item.
     * We first do a null check, since if both are null we can return true.
     * Otherwise, if we didn't read a null, we check the fast path at an offset of 1.
     */
    private fun checkKnownFastNullPath(
        companion: RWPCompanion<*, *>,
        value: Any?,
        buffer: ByteBuffer,
        offset: Int
    ): Boolean {
        return if (AbstractRWP.readNullableHeader(buffer, offset)) {
            return value == null
        } else {
            if (value == null) false else checkKnownFastPath(companion, value, buffer, offset + 1)
        }
    }

    /**
     * If this is called, we have a non-null const-size object.
     * We can always read these from the same position, therefore we avoid a object creation until we have checked if this is a match.
     */
    private fun checkKnownFastPath(companion: RWPCompanion<*, *>, value: Any?, buffer: ByteBuffer, offset: Int): Boolean {
        return companion.compReadFn(buffer, offset) == value
    }

    /**
     * If this is called, we have a non-null const-size object.
     * We can always read these from the same position, therefore we avoid a object creation until we have checked if this is a match.
     */
    private fun checkEqualityFastPath(companion: RWPCompanion<*, *>, function: (Any?) -> Boolean, buffer: ByteBuffer, offset: Int): Boolean {
        return function(companion.compReadFn(buffer, offset))
    }

    /**
     * Returns the number of entries in the database.
     */
    fun getNumberOfEntries(): Long {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB.mdb_txn_begin(env.handle, 0L, 0, pp)

            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_stat(pp[0], handle, stat)

            LMDB.mdb_txn_abort(pp[0])
            return stat.ms_entries()
        }
    }

    /**
     * Returns the size of the database in bytes.
     */
    fun getDBISize(): Long {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB.mdb_txn_begin(env.handle, 0L, 0, pp)

            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_stat(pp[0], handle, stat)

            LMDB.mdb_txn_abort(pp[0])
            return stat.ms_psize() * (stat.ms_branch_pages() + stat.ms_leaf_pages() + stat.ms_overflow_pages())
        }
    }
}
