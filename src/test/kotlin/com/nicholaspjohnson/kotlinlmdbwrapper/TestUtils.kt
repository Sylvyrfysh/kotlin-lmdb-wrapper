package com.nicholaspjohnson.kotlinlmdbwrapper

import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB

object TestUtils {
    fun openDatabase(env: Long, name: String): Int {
        return transaction(env, object : Transaction<Int> {
            override fun exec(stack: MemoryStack, txn: Long): Int {
                val ip = stack.mallocInt(1)

                LMDB_CHECK(LMDB.mdb_dbi_open(txn, name, LMDB.MDB_INTEGERKEY or LMDB.MDB_CREATE, ip))
                return ip.get(0)
            }
        })
    }

    @FunctionalInterface
    interface Transaction<T> {
        fun exec(stack: MemoryStack, txn: Long): T
    }

    fun <T> transaction(env: Long, transaction: Transaction<T>): T {
        var ret: T? = null

        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)

            LMDB_CHECK(LMDB.mdb_txn_begin(env, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            val err: Int
            try {
                ret = transaction.exec(stack, txn)
                err = LMDB.mdb_txn_commit(txn)
            } catch (t: Throwable) {
                LMDB.mdb_txn_abort(txn)
                throw t
            }

            LMDB_CHECK(err)
        }

        return ret!!
    }
}