package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.DataNotFoundException
import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.KeySerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies.ProtoBufSerializeStrategy
import com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies.SerializeStrategy
import kotlinx.serialization.KSerializer
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBStat
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import kotlin.reflect.KProperty1

open class LMDBDbi<DbiType : LMDBObject<DbiType, KeyType>, KeyType: Any>(
    internal val serializer: KSerializer<DbiType>,
    internal val keySerializer: KeySerializer<KeyType>,
    internal val serializeStrategy: SerializeStrategy = ProtoBufSerializeStrategy.DEFAULT,
    useShortName: Boolean = false,
    val name: String = serializer.descriptor.serialName.takeIf(String::isNotBlank)?.let {
        if (useShortName) it.split(".").last() else it
    } ?: error("Must explicitly specify a name for this DBI!"),
    internal val flags: Int = 0,
) {
    internal var handle: Int = -1
        private set
    internal lateinit var env: LMDBEnv

    internal inline val isInit: Boolean
        get() = handle != -1

    @Synchronized
    internal fun onLoadInternal(env: LMDBEnv) {
        check(!isInit) { "Cannot initialize an already initialized dbi!" }
        this.env = env
        preLoad()
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            val ip = stack.mallocInt(1)
            LMDB.mdb_txn_begin(env.handle, MemoryUtil.NULL, 0, pp)
            val dbiFlags = flags or
                    LMDB.MDB_CREATE or
                    (if (keySerializer.needsReverseKey) LMDB.MDB_REVERSEKEY else 0) or
                    (if (keySerializer.isConstSize && (keySerializer.keySize == 4 || keySerializer.keySize == 8)) LMDB.MDB_INTEGERKEY else 0)
            LMDB_CHECK(LMDB.mdb_dbi_open(pp[0], name, dbiFlags, ip))
            handle = ip[0]
            LMDB.mdb_txn_commit(pp[0])
        }

        check(isInit) { "Failed to initialize the database!" }
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
        handle = -1
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
        fun exec(stack: MemoryStack, tx: LMDBTransaction) {
            val pp = stack.mallocPointer(1)

            LMDB_CHECK(LMDB.mdb_cursor_open(tx.tx, handle, pp.position(0)))
            val cursor = pp.get(0)
            val key = MDBVal.mallocStack(stack)
            val data = MDBVal.mallocStack(stack)

            block(cursor, key, data)

            LMDB.mdb_cursor_close(cursor)
        }

        if (readOnly) {
            env.getOrCreateReadTx(::exec)
        } else {
            env.getOrCreateWriteTx(::exec)
        }
    }

    private fun cursorLoop(limit: Long = -1L, after: KeyType? = null, block: (ByteBuffer) -> Unit) {
        cursor { cursor, key, data ->
            var rc = if (after == null) {
                LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_FIRST)
            } else {
                MemoryStack.stackPush().use { stack ->
                    val keyBytes = keySerializer.serialize(after)
                    keyBytes.position(0)
                    key.mv_data()!!.put(keyBytes).position(0)
                    keyBytes.position(0)
                    val int = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_SET_RANGE)
                    if (int != LMDB.MDB_NOTFOUND) {
                        if (key.mv_data()!! == keyBytes) {
                            LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
                        } else {
                            int
                        }
                    } else {
                        int
                    }
                }
            }

            var cnt = 0L
            while (rc != LMDB.MDB_NOTFOUND && cnt++ != limit) {
                LMDB_CHECK(rc)
                val buffer = data.mv_data()!!
                block(buffer)
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
            }
        }
    }

    /**
     * Overload for [writeMultiple] with an [array].
     */
    fun writeMultiple(array: Array<out DbiType>) = writeMultiple(array.iterator())

    /**
     * Overload for [writeMultiple] with an [iterable].
     */
    fun writeMultiple(iterable: Iterable<DbiType>) = writeMultiple(iterable.iterator())

    /**
     * Takes an [iterator], then iterates over it and writes each item to the database.
     */
    fun writeMultiple(iterator: Iterator<DbiType>) {
        check(isInit) { "Cannot modify the database when it is not initialized!" }

        if (!iterator.hasNext()) {
            return
        }
        cursor(false) { cursor, key, data ->
            MemoryStack.stackPush().use { stack ->
                key.mv_data(stack.malloc(0))
                for (item in iterator) {
                    val keyBytes = keySerializer.serialize(item.key)
                    keyBytes.position(0)
                    if (keyBytes.remaining().toLong() > key.mv_size()) {
                        key.mv_data(stack.malloc(keyBytes.remaining()))
                    }
                    with(key.mv_data()!!) {
                        limit(keyBytes.remaining())
                        put(keyBytes)
                        flip()
                    }
                    val bytes = serializeStrategy.serialize(serializer, item)
                    data.mv_size(bytes.size.toLong())
                    LMDB_CHECK(LMDB.mdb_cursor_put(cursor, key, data, LMDB.MDB_RESERVE))

                    data.mv_data()!!.put(bytes)
                }
            }
        }
    }

    fun <M, T : MutableCollection<DbiType>> getElementsWithEqualityTo(
        prop: KProperty1<DbiType, M>,
        value: M,
        to: T,
        limit: Long = -1,
        after: KeyType? = null
    ): T {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoop(limit, after) { buffer ->
            val item = readFromBuffer(buffer)
            if (prop.get(item) == value) {
                to += item
            }
        }

        return to
    }

    fun <M, T : MutableCollection<DbiType>> getElementsWithMemberEqualityFunctionTo(
        prop: KProperty1<DbiType, M>,
        to: T,
        limit: Long = -1L,
        after: KeyType? = null,
        function: (M) -> Boolean
    ): T {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoop(limit, after) { buffer ->
            val item = readFromBuffer(buffer)
            if (function(prop.get(item))) {
                to += item
            }
        }

        return to
    }

    fun <M> getElementsWithMemberEqualityFunction(
        prop: KProperty1<DbiType, M>,
        limit: Long = -1L,
        after: KeyType? = null,
        function: (M) -> Boolean
    ): List<DbiType> {
        return getElementsWithMemberEqualityFunctionTo(prop, ArrayList(), limit, after, function)
    }

    fun <T : MutableCollection<DbiType>> getElementsWithEqualityFunctionTo(
        to: T,
        limit: Long = -1L,
        after: KeyType? = null,
        function: (DbiType) -> Boolean
    ): T {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoop(limit, after) { buffer ->
            val item = readFromBuffer(buffer)
            if (function(item)) {
                to += item
            }
        }

        return to
    }

    fun getElementsWithEqualityFunction(
        limit: Long = -1L,
        after: KeyType? = null,
        function: (DbiType) -> Boolean
    ): List<DbiType> {
        return getElementsWithEqualityFunctionTo(ArrayList(), limit, after, function)
    }

    fun <M> getElementsWithEquality(
        prop: KProperty1<DbiType, M>,
        value: M,
        limit: Long = -1L,
        after: KeyType? = null
    ): List<DbiType> {
        return getElementsWithEqualityTo(prop, value, ArrayList(), limit, after)
    }

    private fun readFromBuffer(buffer: ByteBuffer): DbiType {
        return serializeStrategy.deserialize(serializer, buffer).apply { dbi = this@LMDBDbi }
    }

    private fun cursorLoopKeyRange(
        lowKeyObject: KeyType,
        highKeyObject: KeyType,
        endInclusive: Boolean,
        limit: Long,
        block: (ByteBuffer) -> Unit
    ) {
        val rangeLimit = if (endInclusive) 1 else 0
        cursor { cursor, key, data ->
            MemoryStack.stackPush().use { stack ->
                val txn = LMDB.mdb_cursor_txn(cursor)

                val lowBuffer = keySerializer.serialize(lowKeyObject)
                val highBuffer = keySerializer.serialize(highKeyObject)

                val highKey = MDBVal.mallocStack(stack)
                highKey.mv_data(highBuffer)

                key.mv_data(lowBuffer)
                var rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_SET_RANGE)
                LMDB_CHECK(rc)
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_GET_CURRENT)

                var cnt = 0L
                while (rc != LMDB.MDB_NOTFOUND &&
                    LMDB.mdb_cmp(txn, handle, key, highKey) < rangeLimit &&
                    cnt++ != limit
                ) {
                    LMDB_CHECK(rc)
                    val buffer = data.mv_data()!!
                    block(buffer)
                    rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
                }
            }
        }
    }

    fun <T: MutableCollection<DbiType>> getElementsByKeyRangeTo(
        lowKeyObject: DbiType,
        highKeyObject: DbiType,
        to: T,
        limit: Long = -1L,
        endInclusive: Boolean = false
    ): T {
        return getElementsByKeyRangeTo(lowKeyObject.key, highKeyObject.key, to, limit, endInclusive)
    }

    fun <T: MutableCollection<DbiType>> getElementsByKeyRangeTo(
        lowKeyObject: KeyType,
        highKeyObject: KeyType,
        to: T,
        limit: Long = -1L,
        endInclusive: Boolean = false
    ): T {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoopKeyRange(lowKeyObject, highKeyObject, endInclusive, limit) {
            to += readFromBuffer(it)
        }
        return to
    }

    fun getElementsByKeyRange(
        lowKeyObject: KeyType,
        highKeyObject: KeyType,
        limit: Long = -1L,
        endInclusive: Boolean = false
    ): List<DbiType> {
        return getElementsByKeyRangeTo(lowKeyObject, highKeyObject, ArrayList(), limit, endInclusive)
    }

    fun getElementsByKeyRange(
        lowKeyObject: DbiType,
        highKeyObject: DbiType,
        limit: Long = -1L,
        endInclusive: Boolean = false
    ): List<DbiType> {
        return getElementsByKeyRangeTo(lowKeyObject, highKeyObject, ArrayList(), limit, endInclusive)
    }

    fun forEach(limit: Long = -1L, after: KeyType? = null, block: (DbiType) -> Unit) {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoop(limit, after) {
            block(readFromBuffer(it))
        }
    }

    fun <T : MutableCollection<DbiType>> getEachTo(collection: T, limit: Long = -1L, after: KeyType? = null): T {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        cursorLoop(limit, after) {
            collection += readFromBuffer(it)
        }
        return collection
    }

    fun getEach(limit: Long = -1L, after: KeyType? = null): List<DbiType> {
        return getEachTo(ArrayList(), limit, after)
    }

    /**
     * Returns the number of entries in the database.
     */
    fun getNumberOfEntries(): Long {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        return env.withReadTx<Long> { stack ->
            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_stat(tx.tx, handle, stat)
            return@withReadTx stat.ms_entries()
        }
    }

    /**
     * Returns the size of the database in bytes.
     */
    fun getDBISize(): Long {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        return env.withReadTx<Long> { stack ->
            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_stat(tx.tx, handle, stat)
            return@withReadTx stat.ms_psize() * (stat.ms_branch_pages() + stat.ms_leaf_pages() + stat.ms_overflow_pages())
        }
    }

    fun deleteAllEntries() {
        check(isInit) { "Cannot modify the database when it is not initialized!" }

        env.withWriteTx {
            LMDB.mdb_drop(tx.tx, handle, false)
        }
    }

    fun read(key: KeyType): DbiType {
        check(isInit) { "Cannot query the database when it is not initialized!" }

        return env.getOrCreateReadTx { stack, readTx ->
            val keyBytes = keySerializer.serialize(key)
            val keyBuffer = stack.malloc(keyBytes.remaining())
            keyBuffer.put(keyBytes)
            keyBuffer.position(0)
            val kv = MDBVal.mallocStack().mv_data(keyBuffer)

            val dv = MDBVal.mallocStack()
            val err = LMDB.mdb_get(readTx.tx, handle, kv, dv)
            if (err == LMDB.MDB_NOTFOUND) {
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                LMDB_CHECK(err)
            }
            return@getOrCreateReadTx readFromBuffer(dv.mv_data()!!)
        }
    }
}