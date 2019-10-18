package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.TestUtils.openDatabase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.lwjgl.system.MemoryStack
import org.lwjgl.util.lmdb.LMDB
import java.nio.file.Files
import java.nio.file.Paths

object BasicDBTester {
    private val isCI = System.getenv("CI") != null

    private var env: Long = 0L
    private var dbi: Int = 0
    private val testObj1 = TestObj(ObjectBufferType.New)

    private var nextID: Int = 2
        get() = (field++)

    @BeforeAll
    @JvmStatic
    fun `Set Up`() {
        assumeTrue(!isCI)
        MemoryStack.stackPush().use { stack ->
            val pp = stack.mallocPointer(1)
            LMDB_CHECK(LMDB.mdb_env_create(pp))
            env = pp.get(0)
        }

        LMDB.mdb_env_set_maxdbs(env, 5)
        LMDB_CHECK(
            LMDB.mdb_env_open(
                env,
                Paths.get("db").apply { Files.createDirectories(this) }.toAbsolutePath().toString(),
                0,
                436
            )
        )

        dbi = openDatabase(env, "base")

        testObj1.key = 1
        testObj1.data = 1234
        testObj1.writeInSingleTX(env, dbi)
    }

    @Test
    fun `Test Basic Read Write`() {
        val testObj2 = TestObj(ObjectBufferType.New)
        testObj2.key = 1
        testObj2.readFromDB(env, dbi)
        assertEquals(testObj1.data, testObj2.data)
    }

    @Test
    fun `Test NonExistent Key throws Exception`() {
        val testObj2 = TestObj(ObjectBufferType.New)
        testObj2.key = Integer.MIN_VALUE
        val except = assertThrows<DataNotFoundException> {
            testObj2.readFromDB(env, dbi)
        }
        assertEquals("The key supplied does not have any data in the DB!", except.message)
    }

    @Test
    fun `Test ReWrite Does not Modify Others Until ReRead`() {
        val methodKey = nextID
        val firstData = 5678
        val testObj2 = TestObj(ObjectBufferType.New)
        testObj2.key = methodKey
        testObj2.data = firstData
        testObj2.writeInSingleTX(env, dbi)
        val testObj3 = TestObj(ObjectBufferType.New)
        testObj3.key = methodKey
        testObj3.readFromDB(env, dbi)
        assertEquals(firstData, testObj3.data)
        testObj2.data = 9012
        testObj2.writeInSingleTX(env, dbi)
        assertEquals(firstData, testObj3.data)
        testObj3.readFromDB(env, dbi)
        assertEquals(9012, testObj3.data)
    }

    @Test
    fun `Test Simple Object Size`() {
        assertEquals(9, testObj1.size)
        assertEquals(9, testObj1.minBufferSize)
        assertEquals(9, testObj1.maxBufferSize)
    }

    @Test
    fun `Test data stays null`() {
        val methodKey = nextID
        val testObj2 = TestObj(ObjectBufferType.New)
        testObj2.key = methodKey
        testObj2.data = null
        testObj2.writeInSingleTX(env, dbi)
        val testObj3 = TestObj(ObjectBufferType.New)
        testObj3.key = methodKey
        testObj3.readFromDB(env, dbi)
        assertEquals(null, testObj3.data)
    }
}