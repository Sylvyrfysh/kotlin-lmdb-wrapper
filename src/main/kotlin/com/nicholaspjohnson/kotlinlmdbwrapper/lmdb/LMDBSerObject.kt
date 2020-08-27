package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.DataNotFoundException
import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import org.lwjgl.system.MemoryStack
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBVal

@Serializable
abstract class LMDBSerObject<DbiType : LMDBSerObject<DbiType, KeyType>, KeyType : Any>(@Transient internal var dbi: LMDBSerDbi<DbiType, KeyType>? = null) {
    @Contextual abstract var key: KeyType

    fun write() {
        dbi ?: error("")
        with(dbi!!) {
            val bytes = serializeStrategy.serialize(serializer, this@LMDBSerObject as DbiType)
            env.getOrCreateWriteTx { stack, tx ->
                val kv = getKeyBuffer(stack)

                val dv = MDBVal.mallocStack(stack).mv_size(bytes.size.toLong())
                LMDB_CHECK(LMDB.mdb_put(tx.tx, handle, kv, dv, LMDB.MDB_RESERVE))

                dv.mv_data()!!.put(bytes)
            }
        }
    }

    fun delete() {
        dbi ?: error("")
        with(dbi!!) {
            require(handle != -1) { "DBI must be initialized to delete objects!" }
            env.getOrCreateWriteTx { stack, tx ->
                val kv = getKeyBuffer(stack)

                val data = if ((flags and LMDB.MDB_DUPSORT) == LMDB.MDB_DUPSORT) {
                    val dataBytes = dbi!!.serializeStrategy.serialize(serializer, this@LMDBSerObject as DbiType)
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
                if (err == LMDB.MDB_NOTFOUND) {
                    throw DataNotFoundException("The key supplied does not have any data in the DB!")
                } else {
                    LMDB_CHECK(err)
                }
            }
        }
    }

    private fun getKeyBuffer(stack: MemoryStack): MDBVal {
        val keyBytes = dbi!!.serializeStrategy.serialize(dbi!!.keySerializer, key)

        val key = stack.malloc(keyBytes.size)
        key.put(keyBytes)
        key.position(0)
        return MDBVal.mallocStack(stack).mv_data(key)
    }
}
