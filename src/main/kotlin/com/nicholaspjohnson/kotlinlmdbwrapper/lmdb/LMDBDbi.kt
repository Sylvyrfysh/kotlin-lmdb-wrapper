package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
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

open class LMDBDbi<T : BaseLMDBObject<T>>(
    val name: String,
    val nullStoreOption: NullStoreOption,
    private val constructor: KFunction1<BufferType, T>,
    private val flags: Int = 0
) {
    private lateinit var constOffsets: Map<KProperty1<T, *>, Triple<Int, Boolean, RWPCompanion<*, *>>>

    internal lateinit var nameToIndices: Map<String, Int> //Name to index of position
    internal lateinit var nullables: Array<Boolean>

    internal var handle: Int = -1
        private set
    internal lateinit var env: LMDBEnv

    private var isInit = false

    internal fun onLoad(env: LMDBEnv) {
        require(!isInit) { "Cannot initialize an already initialized dbi!" }
        this.env = env
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
    }

    fun <M> getElementsWithEquality(prop: KProperty1<T, M>, value: M): List<T> {
        require(isInit)
        val (offset, nullable, companion) = constOffsets[prop] ?: Triple(0, false, null)
        val ret = ArrayList<T>()

        val fastPath: Boolean = prop in constOffsets
        val fastMethod = if (nullable) {
                ::checkFastNullPath
            } else {
                ::checkFastPath
            }

        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)

            LMDB_CHECK(LMDB.mdb_txn_begin(env.handle, MemoryUtil.NULL, LMDB.MDB_RDONLY, pp))
            val txn = pp.get(0)

            LMDB_CHECK(LMDB.mdb_cursor_open(txn, handle, pp.position(0)))
            val cursor = pp.get(0)

            val data = MDBVal.mallocStack(stack)
            val key = MDBVal.mallocStack(stack)

            var rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_FIRST)

            while (rc != LMDB.MDB_NOTFOUND) {
                LMDB_CHECK(rc)
                val buffer = data.mv_data()!!
                if (fastPath) {
                    if (fastMethod.invoke(companion, value, buffer, offset)) {
                        ret += constructor(BufferType.DBRead(buffer))
                    }
                } else {
                    val item = constructor(BufferType.DBRead(buffer))
                    if (checkSlowPath(prop, value, item)) {
                        ret += item
                    }
                }
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
            }

            LMDB.mdb_cursor_close(cursor)
            LMDB.mdb_txn_abort(txn)
            return ret
        }
    }

    /**
     * If this is called, [prop] is a non-const size property.
     * This means that we need to load the whole object to make sure we have the correct offsets.
     */
    private fun checkSlowPath(prop: KProperty1<T, *>, value: Any?, item: T): Boolean {
        return prop.get(item) == value
    }

    /**
     * If this is called, we have a speed-optimized database with a potentially null item.
     * We first do a null check, since if both are null we can return true.
     * Otherwise, if we didn't read a null, we check the fast path at an offset of 1.
     */
    private fun checkFastNullPath(companion: RWPCompanion<*, *>?, value: Any?, buffer: ByteBuffer, offset: Int): Boolean {
        return if (AbstractRWP.readNullableHeader(buffer, offset)) {
            return value == null
        } else {
            if (value == null) false else checkFastPath(companion, value, buffer, offset + 1)
        }
    }

    /**
     * If this is called, we have a non-null const-size object.
     * We can always read these from the same position, therefore we avoid a object creation until we have checked if this is a match.
     */
    private fun checkFastPath(companion: RWPCompanion<*, *>?, value: Any?, buffer: ByteBuffer, offset: Int): Boolean {
        return companion!!.compReadFn(buffer, offset) == value
    }

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
}
