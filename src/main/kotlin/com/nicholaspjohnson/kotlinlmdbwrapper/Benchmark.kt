package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBDbi
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBEnv
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.LongKeySerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializers.UUIDKeySerializer
import com.nicholaspjohnson.kotlinlmdbwrapper.serializestrategies.ProtoBufSerializeStrategy
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.serializer
import org.lwjgl.system.Configuration
import org.lwjgl.util.lmdb.LMDB
import java.nio.file.Paths
import java.security.SecureRandom
import java.util.*
import kotlin.random.Random
import kotlin.system.measureTimeMillis

object Benchmark {
    @JvmStatic
    fun main(args: Array<String>) {
        Configuration.DEBUG_STACK.set(true)
        Configuration.DEBUG_MEMORY_ALLOCATOR.set(true)
        Configuration.DEBUG_MEMORY_ALLOCATOR_INTERNAL.set(true)
        val env2 = LMDBEnv(Paths.get("serenvbenchdb"))
        env2.openDbi(BenchObj2)

        fun clearDB() {
            BenchObj2.deleteAllEntries()
        }

        fun createAndWrite1000(run: Int) {
            var objs: Array<BenchObj2> = emptyArray()
            val rand = Random.nextBytes(16)
            val createTime = measureTimeMillis {
                objs = Array(100000) {
                    val obj = BenchObj2()
                    obj.key = it.toLong()
                    obj.data = rand
                    obj
                }
            }
            val writeTime = measureTimeMillis {
                env2.withWriteTx {
                    BenchObj2.writeMultiple(objs)
                }
            }
            val findRandTime = measureTimeMillis {
                BenchObj2.getElementsWithEquality(BenchObj2::data, Random.nextBytes(16))
            }
            val findFixTime = measureTimeMillis {
                BenchObj2.getElementsWithEquality(BenchObj2::key, 25L)
            }
            val findKeyTime = measureTimeMillis {
                BenchObj2.getElementsByKeyRange(100L, 200L)
            }
            println(BenchObj2.getDBISize())
            val deleteTime = measureTimeMillis {
                clearDB()
            }
            println("Run Number $run Time: Create: $createTime ms, Write: $writeTime ms, Find Rand: $findRandTime ms, Find Fixed: $findFixTime ms, Find Key Range: $findKeyTime ms, Delete: $deleteTime ms")
        }

        fun create1AndWrite1000(run: Int) {
            val obj = BenchObj2()
            obj.data = Random.nextBytes(16)
            val writeTime = measureTimeMillis {
                env2.withWriteTx {
                    repeat(1000) {
                        obj.key = it.toLong()
                        obj.write()
                    }
                }
            }
            val deleteTime = measureTimeMillis {
                clearDB()
            }
            println("Run Number $run Time: Write: $writeTime ms, Delete: $deleteTime ms")
        }

        repeat(8) {
            create1AndWrite1000(it)
        }
        repeat(8) {
            createAndWrite1000(it)
        }

        env2.close()

        UUIDKeySerializer().serialize(generateNewID())
        UUIDKeySerializer().serialize(generateNewID())
        Thread.sleep(50)
        UUIDKeySerializer().serialize(generateNewID())
        UUIDKeySerializer().serialize(generateNewID())
        Thread.sleep(50)
        UUIDKeySerializer().serialize(generateNewID())
        UUIDKeySerializer().serialize(generateNewID())

        for (number in longArrayOf(0, 3, 255, 15674867895746L)) {
            println("Number $number")
            println(number.toString(16).padStart(16, '0'))
            println(
                number.toString().toCharArray()
                    .joinToString("") { it.toInt().and(0xFF).toString(16).padStart(2, '0') })
            println(
                ProtoBufSerializeStrategy.DEFAULT.serialize(Long.serializer(), number)
                    .joinToString("") { it.toInt().and(0xFF).toString(16).padStart(2, '0') })
        }
    }

    @Serializable
    class BenchObj2 : LMDBObject<BenchObj2, Long>(Companion) {
        override var key: Long = 0L
        var data: ByteArray = ByteArray(0)

        companion object : LMDBDbi<BenchObj2, Long>(serializer(), LongKeySerializer, flags = LMDB.MDB_INTEGERKEY)
    }

    private val sr: ThreadLocal<java.util.Random> = ThreadLocal.withInitial(::SecureRandom)

    private const val NUM_RANDOM_BYTES = 10

    private const val MSB_TAKE_BYTES = NUM_RANDOM_BYTES - Long.SIZE_BYTES

    private const val BYTES_FOR_TIME = 16 - NUM_RANDOM_BYTES
    private const val BITS_TO_SHIFT_TIME = (8 - BYTES_FOR_TIME) * 8

    private fun generateNewID(): UUID {
        val uuidBytes = ByteArray(NUM_RANDOM_BYTES)
        sr.get().nextBytes(uuidBytes)

        var msb = uuidBytes.take(MSB_TAKE_BYTES).fold(0L) { acc, byte -> (acc shl 8) + (byte.toLong() and 0xFFL) }
        val lsb = uuidBytes.takeLast(8).fold(0L) { acc, byte -> (acc shl 8) + (byte.toLong() and 0xFFL) }

        msb = msb or (System.currentTimeMillis() shl BITS_TO_SHIFT_TIME)

        return UUID(msb, lsb)
    }
}