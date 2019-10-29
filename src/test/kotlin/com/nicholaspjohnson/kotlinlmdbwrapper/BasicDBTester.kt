package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.TestUtils.openDatabase
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.AbstractRWP
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.lwjgl.system.MemoryStack
import org.lwjgl.util.lmdb.LMDB
import java.lang.IllegalStateException
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import kotlin.reflect.KClass

object BasicDBTester {
    private val isCI = System.getenv("CI") != null

    private var env: Long = 0L
    private var dbi: Int = 0
    private val testObj1 = TestObj(ObjectBufferType.None)

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
        val testObj2 = TestObj(ObjectBufferType.None)
        testObj2.key = 1
        testObj2.readFromDB(env, dbi)
        assertEquals(testObj1.data, testObj2.data)
    }

    @Test
    fun `Test NonExistent Key throws Exception`() {
        val testObj2 = TestObj(ObjectBufferType.None)
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
        val testObj2 = TestObj(ObjectBufferType.None)
        testObj2.key = methodKey
        testObj2.data = firstData
        testObj2.writeInSingleTX(env, dbi)
        val testObj3 = TestObj(ObjectBufferType.None)
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
    fun `Test data stays null`() {
        val methodKey = nextID
        val testObj2 = TestObj(ObjectBufferType.None)
        testObj2.key = methodKey
        testObj2.data = null
        testObj2.writeInSingleTX(env, dbi)
        val testObj3 = TestObj(ObjectBufferType.None)
        testObj3.key = methodKey
        testObj3.readFromDB(env, dbi)
        assertEquals(null, testObj3.data)
    }

    @Test
    fun `Test data offsets after load`() {
        val methodKey = nextID
        val mixNormalNulls = MixNormalNulls()
        mixNormalNulls.normInt = methodKey
        mixNormalNulls.nullableInt = 35
        mixNormalNulls.aNullableString = "This is a string first."
        mixNormalNulls.normalString = "This is something I guess"
        mixNormalNulls.aNullableString = null
        mixNormalNulls.writeInSingleTX(env, dbi)

        val mixNormalNulls2 = MixNormalNulls()
        mixNormalNulls2.normInt = methodKey
        mixNormalNulls2.readFromDB(env, dbi)

        assertEquals(methodKey, mixNormalNulls2.normInt)
        Assertions.assertNotNull(mixNormalNulls2.nullableInt)
        assertEquals(35, mixNormalNulls2.nullableInt!!)
        assertEquals("This is something I guess", mixNormalNulls2.normalString)
        Assertions.assertNull(mixNormalNulls2.aNullableString)
    }

    @Test
    fun `Test shrink VarSize does not break`() {
        val methodKey = nextID
        val fs1 = MultipleVarLongs()
        fs1.first = Long.MAX_VALUE
        fs1.second = 2
        fs1.first = methodKey.toLong()
        fs1.writeInSingleTX(env, dbi)

        val fs2 = MultipleVarLongs()
        fs2.first = methodKey.toLong()
        fs2.readFromDB(env, dbi)

        assertEquals(2L, fs2.second)
    }

    @Test
    fun `Test that all stay expected with DB rw`() {
        val methodKey = nextID.toLong()

        val bool = true
        val byte = 23.toByte()
        val short = 652.toShort()
        val char = 'Z'
        val int = 74482
        val float = 2578904.245f
        val long = methodKey
        val double = 31876915.31568135
        val varlong = 156937804914256L
        val varchar = "This is a testing string. It isn't that special."

        val ato = AllTypesObject()
        ato.bool = bool
        ato.byte = byte
        ato.short = short
        ato.char = char
        ato.int = int
        ato.float = float
        ato.long = long
        ato.double = double
        ato.varlong = varlong
        ato.varchar = varchar

        ato.writeInSingleTX(env, dbi)

        val ato2 = AllTypesObject()
        ato2.long = methodKey

        ato2.readFromDB(env, dbi)

        assertEquals(bool, ato2.bool)
        assertEquals(byte, ato2.byte)
        assertEquals(short, ato2.short)
        assertEquals(char, ato2.char)
        assertEquals(int, ato2.int)
        assertEquals(float, ato2.float)
        assertEquals(long, ato2.long)
        assertEquals(double, ato2.double)
        assertEquals(varlong, ato2.varlong)
        assertEquals(varchar, ato2.varchar)
    }

    @Test
    fun `Test basic ByteArray size setting and loading`() {
        val methodKey = nextID.toLong()
        val testArr = ByteArray(32, Int::toByte)

        val bbo = ByteArrayTesterObject()
        bbo.buffer = testArr
        bbo.zInt = 5
        bbo.key = methodKey

        bbo.writeInSingleTX(env, dbi)

        val bbo2 = ByteArrayTesterObject()
        bbo2.key = methodKey
        assertNull(bbo2.buffer)

        bbo2.readFromDB(env, dbi)

        assertArrayEquals(testArr, bbo2.buffer)
    }

    @Test
    fun `Test ShortArray offset 1`() {
        val methodKey = nextID.toLong()
        val expectArray = ShortArray(16, Int::toShort)

        val writeObj = MisalignedShortArray()
        writeObj.key = methodKey
        writeObj.single = 25.toByte()
        writeObj.zArray = expectArray

        writeObj.writeInSingleTX(env, dbi)

        val readObj = MisalignedShortArray()
        readObj.key = methodKey
        readObj.readFromDB(env, dbi)

        assertArrayEquals(expectArray, readObj.zArray)
    }

    @Test
    fun `Test list basics`() {
        val methodKey = nextID.toLong()
        val expectList = arrayListOf("Why", "like", "This? IDK")

        val listObj = ListTester()
        listObj.key = methodKey
        listObj.list = expectList

        listObj.writeInSingleTX(env, dbi)

        val listObj2 = ListTester()
        listObj2.key = methodKey
        listObj2.readFromDB(env, dbi)

        assertLinesMatch(expectList, listObj2.list)
    }

    @Test
    fun `Test custom RWP`() {
        assertThrows<IllegalStateException>("There is no RWP for the type UUID!") { CustomUUIDRWP() }

        val methodKey = nextID.toLong()
        LMDBBaseObjectProvider.addRWP(UUID::class, UUIDRWP::class)
        val expectUUID = UUID.randomUUID()

        val customRWPObject = CustomUUIDRWP()
        customRWPObject.key = methodKey
        customRWPObject.uuid = expectUUID
        customRWPObject.writeInSingleTX(env, dbi)

        val cObj2 = CustomUUIDRWP()
        cObj2.key = methodKey
        cObj2.readFromDB(env, dbi)

        assertEquals(expectUUID, cObj2.uuid)
    }
}