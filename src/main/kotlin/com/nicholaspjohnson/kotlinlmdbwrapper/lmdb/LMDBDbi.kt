package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import com.nicholaspjohnson.kotlinlmdbwrapper.BufferType
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import kotlin.reflect.KFunction1

open class LMDBDbi<T: BaseLMDBObject<T>>(private val name: String, val nullStoreOption: NullStoreOption, private val constructor: KFunction1<BufferType, T>, private val flags: Int = 0) {
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

        val obj = constructor(BufferType.None)
        obj.setUsed()
        val constOffsets = obj.constSizeMap

        isInit = true
    }
}
