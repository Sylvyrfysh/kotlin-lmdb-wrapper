package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDB_CHECK
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class LMDBEnv (private val path: Path, startingSize: Long = 1L * 1024 * 1024, numDbis: Int = 8, envFlags: Int = 0) {
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
                Paths.get("db").apply {
                    if (!Files.exists(path)) {
                        Files.createDirectories(path)
                    } else {
                        require(Files.isDirectory(path)) {
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

    fun openDbi(dbi: LMDBDbi<*>) {
        dbi.onLoad(this)
    }

    fun close() {
        LMDB.mdb_env_close(handle)
        handle = -1
    }
}