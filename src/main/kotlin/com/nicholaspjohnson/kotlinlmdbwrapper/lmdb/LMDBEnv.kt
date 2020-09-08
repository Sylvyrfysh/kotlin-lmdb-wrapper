package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBEnvInfo
import org.lwjgl.util.lmdb.MDBStat
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashSet

/**
 * Kotlin representation of the LMDB environment.
 *
 * @constructor
 *
 * Creates a new LMDB environment at [path].
 */
open class LMDBEnv(
    private val path: Path,
    startingSize: Long = 64L * 1024 * 1024,
    numDbis: Int = 8,
    envFlags: Int = 0,
    val requireExplicitTx: Boolean = false
) {
    @PublishedApi
    internal var handle: Long = -1
        private set

    private val openDBIs = HashSet<LMDBDbi<*, *>>()

    var sizeInBytes: Long
        private set

    init {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB_CHECK(LMDB.mdb_env_create(pp))
            handle = pp.get(0)
        }

        //we keep track of some internal state here as well about the dbis
        LMDB.mdb_env_set_maxdbs(handle, numDbis)
        LMDB.mdb_env_set_mapsize(handle, startingSize)
        LMDB_CHECK(
            LMDB.mdb_env_open(
                handle,
                path.apply {
                    if (!Files.exists(this)) {
                        if ((envFlags and LMDB.MDB_NOSUBDIR) != LMDB.MDB_NOSUBDIR) {
                            Files.createDirectories(this)
                        }
                    } else {
                        require(Files.isDirectory(this) || (envFlags and LMDB.MDB_NOSUBDIR) == LMDB.MDB_NOSUBDIR) {
                            LMDB.mdb_env_close(handle)
                            "Path must be a directory, or envFlags must have LMDB.MDB_NOSUBDIR!"
                        }
                    }
                }.toAbsolutePath().toString(),
                envFlags,
                436 // 0644 in decimal
            )
        )

        MemoryStack.stackPush().use { stack ->
            val stat = MDBEnvInfo.mallocStack(stack)
            LMDB.mdb_env_info(handle, stat)
            sizeInBytes = stat.me_mapsize()
        }
    }

    /**
     * Returns the size of the environment metadata in bytes, including the database lookup table.
     */
    fun getEnvMetadataSize(): Long {
        MemoryStack.stackPush().use { stack ->
            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_env_stat(handle, stat)

            return stat.ms_psize() * (stat.ms_branch_pages() + stat.ms_leaf_pages() + stat.ms_overflow_pages())
        }
    }

    /**
     * Returns the size of the environment and all of the open DBI's in bytes.
     */
    fun getTotalSizeWithOpenDBIs(): Long {
        return getEnvMetadataSize() + openDBIs.map(LMDBDbi<*, *>::getDBISize).sum()
    }

    /**
     * Opens [dbi], running internal initialization logic.
     *
     * Throws an [IllegalStateException] if the DBI is already open.
     */
    fun openDbi(dbi: LMDBDbi<*, *>) {
        synchronized(openDBIs) {
            check(openDBIs.add(dbi)) { "Cannot open a dbi which is already open!" }
            dbi.onLoadInternal(this)
        }
    }

    /**
     * Closes [dbi], running internal closure logic.
     *
     * Throws an [IllegalStateException] if the DBI is not open.
     */
    fun closeDbi(dbi: LMDBDbi<*, *>) {
        synchronized(openDBIs) {
            check(openDBIs.remove(dbi)) { "Cannot close a dbi which is not open!" }
            dbi.onCloseInternal(this)
        }
    }

    /**
     * Closes this environment and all open [LMDBDbi]'s, running internal closure logic.
     *
     * Throws an [IllegalStateException] if the environment is not open.
     */
    fun close() {
        check(handle != -1L) { "The environment is not open!" }
        synchronized(openDBIs) {
            openDBIs.retainAll { it.onCloseInternal(this); false }
            LMDB.mdb_env_close(handle)
            handle = -1
        }
        internalTx = ThreadLocal.withInitial { Stack<Pair<MemoryStack, LMDBTransaction>>() }
    }

    @PublishedApi
    internal var internalTx: ThreadLocal<Stack<Pair<MemoryStack, LMDBTransaction>>> =
        ThreadLocal.withInitial { Stack<Pair<MemoryStack, LMDBTransaction>>() }

    internal inline fun getOrCreateWriteTx(crossinline block: (MemoryStack, LMDBTransaction) -> Unit) {
        if (internalTx.get().empty()) {
            check(!requireExplicitTx) { "There is no live write transaction, and explicit transactions are required!" }
            withWriteTx {
                val (stack, tx) = internalTx.get().peek()
                block(stack, tx)
            }
        } else {
            val (stack, tx) = internalTx.get().peek()
            check(!tx.isReadOnly) { "The most recent transaction is not a write transaction, but a write was called!" }
            block(stack, tx)
        }
    }

    internal inline fun <T> getOrCreateReadTx(crossinline block: (MemoryStack, LMDBTransaction) -> T): T {
        if (internalTx.get().empty()) {
            check(!requireExplicitTx) { "There is no live read transaction, and explicit transactions are required!" }
            return withReadTx<T> {
                val (stack, tx) = internalTx.get().peek()
                return@withReadTx block(stack, tx)
            }
        } else {
            val (stack, tx) = internalTx.get().peek()
            return block(stack, tx)
        }
    }

    inline fun withReadTx(crossinline block: ReadTXScope<Unit>.(MemoryStack) -> Unit) {
        return withReadTx<Unit>(block)
    }

    inline fun <T> withReadTx(crossinline block: ReadTXScope<T>.(MemoryStack) -> T): T {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB_CHECK(
                LMDB.mdb_txn_begin(
                    handle,
                    if (internalTx.get().empty()) 0L else internalTx.get().peek().second.tx,
                    LMDB.MDB_RDONLY,
                    pp
                )
            )
            val txn = pp.get(0)
            val rtx = object : ReadTXScope<T>(LMDBTransaction(true, txn)) {
                override fun exec(stack: MemoryStack) = block(stack)
            }
            internalTx.get().push(stack to rtx.tx)
            try {
                return rtx.exec(stack)
            } catch (t: Throwable) {
                rtx.abortSilent()
                throw t
            } finally {
                rtx.close()
                internalTx.get().pop()
            }
        }
    }

    inline fun withWriteTx(crossinline block: WriteTXScope<Unit>.(MemoryStack) -> Unit) {
        return withWriteTx<Unit>(block)
    }

    inline fun <T> withWriteTx(crossinline block: WriteTXScope<T>.(MemoryStack) -> T): T {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB_CHECK(
                LMDB.mdb_txn_begin(
                    handle,
                    if (internalTx.get().empty()) 0L else internalTx.get().peek().second.also { check(!it.isReadOnly) { "Cannot nest a write transaction in a read transaction!" } }.tx,
                    0,
                    pp
                )
            )
            val txn = pp.get(0)
            val rtx = object : WriteTXScope<T>(LMDBTransaction(false, txn)) {
                override fun exec(stack: MemoryStack) = block(stack)
            }
            internalTx.get().push(stack to rtx.tx)
            try {
                return rtx.exec(stack)
            } catch (t: Throwable) {
                rtx.abortSilent()
                throw t
            } finally {
                rtx.close()
                internalTx.get().pop()
            }
        }
    }

    fun getAllDBINames(): List<String> {
        return MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)

            val ip = stack.mallocInt(1)
            LMDB.mdb_txn_begin(handle, MemoryUtil.NULL, 0, pp)
            LMDB_CHECK(LMDB.mdb_dbi_open(pp[0], null as ByteBuffer?, 0, ip))
            val dbHandle = ip[0]
            LMDB.mdb_txn_commit(pp[0])

            LMDB_CHECK(LMDB.mdb_txn_begin(handle, MemoryUtil.NULL, LMDB.MDB_RDONLY, pp))
            val txn = pp.get(0)

            LMDB_CHECK(LMDB.mdb_cursor_open(txn, dbHandle, pp.position(0)))
            val cursor = pp.get(0)

            val key = MDBVal.mallocStack(stack)
            val data = MDBVal.mallocStack(stack)

            var rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_FIRST)

            val ret = ArrayList<String>()
            while (rc != LMDB.MDB_NOTFOUND) {
                LMDB_CHECK(rc)
                ret += Charsets.UTF_8.decode(key.mv_data()!!).toString()
                rc = LMDB.mdb_cursor_get(cursor, key, data, LMDB.MDB_NEXT)
            }

            LMDB.mdb_cursor_close(cursor)
            LMDB.mdb_txn_abort(txn)

            return@use ret
        }
    }
}