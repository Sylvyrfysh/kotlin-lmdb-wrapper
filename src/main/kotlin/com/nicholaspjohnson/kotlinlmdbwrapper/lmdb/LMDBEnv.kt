package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import org.lwjgl.util.lmdb.MDBStat
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Creates a new LMDB environment at [path] with a [startingSize], [numDbis] and with [envFlags].
 */
open class LMDBEnv (private val path: Path, startingSize: Long = 1L * 1024 * 1024, numDbis: Int = 8, envFlags: Int = 0) {
    internal var handle: Long = -1
        private set
    init {
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB_CHECK(LMDB.mdb_env_create(pp))
            handle = pp.get(0)
        }

        LMDB.mdb_env_set_maxdbs(handle, numDbis)
        LMDB.mdb_env_set_mapsize(handle, startingSize)
        LMDB_CHECK(
            LMDB.mdb_env_open(
                handle,
                path.apply {
                    if (!Files.exists(this)) {
                        Files.createDirectories(this)
                    } else {
                        require(Files.isDirectory(this)) {
                            LMDB.mdb_env_close(handle)
                            "Path must be a directory!"
                        }
                    }
                }.toAbsolutePath().toString(),
                envFlags,
                436 // 0644 in decimal
            )
        )
    }

    /**
     * Returns the size of the environment metadata in bytes, including the database lookup table.
     */
    fun getEnvMetadataSize(): Long {
        MemoryStack.stackPush().use { stack ->
            val stat = MDBStat.mallocStack(stack)
            LMDB.mdb_env_stat(handle, stat)

            return stat.ms_psize() * (stat.ms_branch_pages() + stat.ms_leaf_pages() * stat.ms_overflow_pages())
        }
    }

    /**
     * Returns the size of the environment and all of the open DBI's in bytes.
     */
    fun getTotalSizeWithOpenDBIs(): Long {
        return getEnvMetadataSize() + openDBIs.map(LMDBDbi<*>::getDBISize).sum()
    }

    private val openDBIs = HashSet<LMDBDbi<*>>()

    /**
     * Opens [dbi], running internal initialization logic.
     * Throws an [IllegalStateException] if the DBI is already open.
     */
    fun openDbi(dbi: LMDBDbi<*>) {
        synchronized(openDBIs) {
            check(openDBIs.add(dbi)) { "Cannot open a dbi which is already open!" }
            dbi.onLoad(this)
        }
    }

    /**
     * Closes [dbi], running internal closure logic.
     * Throws an [IllegalStateException] if the DBI is not open.
     */
    fun closeDbi(dbi: LMDBDbi<*>) {
        synchronized(openDBIs) {
            check(openDBIs.remove(dbi)) { "Cannot close a dbi which is not open!" }
            dbi.onClose(this)
        }
    }

    /**
     * Closes this environment and all open [LMDBDbi]'s, running internal closure logic.
     * Throws an [IllegalStateException] if the environment is not open.
     */
    fun close() {
        check(handle != -1L) { "The environment is not open!" }
        synchronized(openDBIs) {
            openDBIs.retainAll { it.onClose(this); false }
            LMDB.mdb_env_close(handle)
            handle = -1
        }
    }
}