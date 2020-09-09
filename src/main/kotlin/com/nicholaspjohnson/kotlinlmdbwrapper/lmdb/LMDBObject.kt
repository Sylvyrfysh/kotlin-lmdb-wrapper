package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.DataNotFoundException
import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBVal

@Serializable
abstract class LMDBObject<DbiType : LMDBObject<DbiType, KeyType>, KeyType : Any>(
    @Transient internal var dbi: LMDBDbi<DbiType, KeyType>? = null
) {
    /**
     * The key that should be used as the database key.
     */
    abstract val key: KeyType

    fun write() {
        check(dbi?.isInit == true) { "Cannot modify the database when it is not initialized!" }

        with(dbi!!) {
            val bytes = serializeStrategy.serialize(serializer, this@LMDBObject as DbiType)
            env.getOrCreateWriteTx { stack, tx ->
                val kv = getKeyBuffer(stack)

                val dv = MDBVal.mallocStack(stack).mv_size(bytes.size.toLong())
                LMDB_CHECK(LMDB.mdb_put(tx.tx, handle, kv, dv, LMDB.MDB_RESERVE))
                if (keySerializer.needsFree) {
                    MemoryUtil.memFree(kv.mv_data()!!)
                }

                dv.mv_data()!!.put(bytes)
            }
        }
    }

    fun delete() {
        check(dbi?.isInit == true) { "Cannot modify the database when it is not initialized!" }

        with(dbi!!) {
            require(handle != -1) { "DBI must be initialized to delete objects!" }
            env.getOrCreateWriteTx { stack, tx ->
                val kv = getKeyBuffer(stack)

                val data = if ((flags and LMDB.MDB_DUPSORT) == LMDB.MDB_DUPSORT) {
                    val dataBytes = serializeStrategy.serialize(serializer, this@LMDBObject as DbiType)
                    val data = stack.malloc(dataBytes.size)
                    data.put(dataBytes)
                    data.position(0)

                    val dv = MDBVal.mallocStack(stack)
                    dv.mv_data(data)

                    dv
                } else {
                    null
                }

                val err = LMDB.mdb_del(tx.tx, handle, kv, data)
                if (keySerializer.needsFree) {
                    MemoryUtil.memFree(kv.mv_data()!!)
                }
                if (err == LMDB.MDB_NOTFOUND) {
                    throw DataNotFoundException("The key supplied does not have any data in the DB!")
                } else {
                    LMDB_CHECK(err)
                }
            }
        }
    }

    private fun getKeyBuffer(stack: MemoryStack): MDBVal {
        return MDBVal.mallocStack(stack).mv_data(dbi!!.keySerializer.serialize(key).apply { position(0) })
    }
}
