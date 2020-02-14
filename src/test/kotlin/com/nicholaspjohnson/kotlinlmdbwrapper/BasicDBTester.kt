package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBEnv
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

object BasicDBTester {
    private val isCI = System.getenv("CI") != null

    private lateinit var env: LMDBEnv
    private val testObj1 = TestObj()

    private var nextID: Int = 2
        get() = (field++)

    @BeforeAll
    @JvmStatic
    fun `Set Up`() {
        assumeTrue(!isCI)

        if (Files.exists(Paths.get("db"))) {
            Files.list(Paths.get("db")).forEach(Files::delete)
        } else {
            Files.createDirectories(Paths.get("db"))
        }

        env = LMDBEnv(Paths.get("db"), numDbis = 32)
        env.openDbi(TestObj)
        env.openDbi(MixNormalNulls)
        env.openDbi(MultipleVarLongs)
        env.openDbi(AllTypesObject)
        env.openDbi(MultiWrite)
        env.openDbi(DefaultSetTesterObject)
        env.openDbi(ByteArrayTesterObject)
        env.openDbi(MisalignedShortArray)
        RWPProvider.addRWP(UUID::class, UUIDRWP::class)
        env.openDbi(CustomUUIDRWP)
        env.openDbi(ListTester)
        env.openDbi(MapTester)

        testObj1.key = 1
        testObj1.data = 1234
        testObj1.writeInSingleTX()
    }
    
    @AfterAll
    @JvmStatic
    fun `Tear Down`() {
        assumeTrue(!isCI)

        //LMDB.mdb_env_close(env)
    }

    @Test
    fun `Test Basic Read Write`() {
        val testObj2 = TestObj()
        testObj2.key = 1
        testObj2.readFromDB()
        assertEquals(testObj1.data, testObj2.data)
    }

    @Test
    fun `Test NonExistent Key throws Exception`() {
        val testObj2 = TestObj()
        testObj2.key = Integer.MIN_VALUE
        val except = assertThrows<DataNotFoundException> {
            testObj2.readFromDB()
        }
        assertEquals("The key supplied does not have any data in the DB!", except.message)
    }

    @Test
    fun `Test ReWrite Does not Modify Others Until ReRead`() {
        val methodKey = nextID
        val firstData = 5678
        val testObj2 = TestObj()
        testObj2.key = methodKey
        testObj2.data = firstData
        testObj2.writeInSingleTX()
        val testObj3 = TestObj()
        testObj3.key = methodKey
        testObj3.readFromDB()
        assertEquals(firstData, testObj3.data)
        testObj2.data = 9012
        testObj2.writeInSingleTX()
        assertEquals(firstData, testObj3.data)
        testObj3.readFromDB()
        assertEquals(9012, testObj3.data)
    }

    @Test
    fun `Test data stays null`() {
        val methodKey = nextID
        val testObj2 = TestObj()
        testObj2.key = methodKey
        testObj2.data = null
        testObj2.writeInSingleTX()
        val testObj3 = TestObj()
        testObj3.key = methodKey
        testObj3.readFromDB()
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
        mixNormalNulls.writeInSingleTX()

        val mixNormalNulls2 = MixNormalNulls()
        mixNormalNulls2.normInt = methodKey
        mixNormalNulls2.readFromDB()

        assertEquals(methodKey, mixNormalNulls2.normInt)
        assertNotNull(mixNormalNulls2.nullableInt)
        assertEquals(35, mixNormalNulls2.nullableInt!!)
        assertEquals("This is something I guess", mixNormalNulls2.normalString)
        assertNull(mixNormalNulls2.aNullableString)
    }

    @Test
    fun `Test shrink VarSize does not break`() {
        val methodKey = nextID
        val fs1 = MultipleVarLongs()
        fs1.first = Long.MAX_VALUE
        fs1.second = 2
        fs1.first = methodKey.toLong()
        fs1.writeInSingleTX()

        val fs2 = MultipleVarLongs()
        fs2.first = methodKey.toLong()
        fs2.readFromDB()

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
        val nullableInt: Int? = 898

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
        ato.nullableInt = nullableInt

        ato.writeInSingleTX()

        val ato2 = AllTypesObject()
        ato2.long = methodKey

        ato2.readFromDB()

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
        assertEquals(nullableInt, ato2.nullableInt)

        assertEquals(1, AllTypesObject.getElementsWithEquality(AllTypesObject::bool, ato.bool).size)
        assertEquals(1, AllTypesObject.getElementsWithEquality(AllTypesObject::varlong, ato.varlong).size)
        assertEquals(1, AllTypesObject.getElementsWithEquality(AllTypesObject::nullableInt, ato.nullableInt).size)
        assertEquals(0, AllTypesObject.getElementsWithEquality(AllTypesObject::nullableInt, null).size)
    }

    @Test
    fun `Test write multiple`() {
        val itemsToWrite = Array(10) {
            val mw = MultiWrite()
            mw.key = it.toLong()
            mw.data = 100L + (it * 10)
            mw
        }

        assertEquals(0, MultiWrite.getNumberOfEntries())

        MultiWrite.writeMultiple(itemsToWrite)

        assertEquals(10, MultiWrite.getNumberOfEntries())

        assertEquals(5, MultiWrite.getElementsByKeyRange(MultiWrite().apply { key = 0L }, MultiWrite().apply { key = 5L }).size)
        assertEquals(6, MultiWrite.getElementsByKeyRange(MultiWrite().apply { key = 0L }, MultiWrite().apply { key = 5L }, endInclusive = true).size)

        assertEquals(1, MultiWrite.getElementsWithEquality(MultiWrite::data, 110).size)
        assertEquals(0, MultiWrite.getElementsWithEquality(MultiWrite::data, 111).size)

        assertEquals(3, MultiWrite.getElementsWithEqualityFunction(MultiWrite::data) { it >= 170 }.size)
    }

    @Test
    fun `Default Set`() {
        val testObj = DefaultSetTesterObject()
        assertEquals(DefaultSetTesterObject.initialSet, testObj.shouldBeDefault)
    }

    @Test
    fun `Test basic ByteArray size setting and loading`() {
        val methodKey = nextID.toLong()
        val testArr = ByteArray(32, Int::toByte)

        val bbo = ByteArrayTesterObject()
        bbo.buffer = testArr
        bbo.zInt = 5
        bbo.key = methodKey

        bbo.writeInSingleTX()

        val bbo2 = ByteArrayTesterObject()
        bbo2.key = methodKey
        assertNull(bbo2.buffer)

        bbo2.readFromDB()

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

        writeObj.writeInSingleTX()

        val readObj = MisalignedShortArray()
        readObj.key = methodKey
        readObj.readFromDB()

        assertArrayEquals(expectArray, readObj.zArray)
    }

    @Test
    fun `Test list basics`() {
        val methodKey = nextID.toLong()
        val expectList = arrayListOf("Why", "like", "This? IDK")

        val listObj = ListTester()
        listObj.key = methodKey
        listObj.list = expectList

        listObj.writeInSingleTX()

        val listObj2 = ListTester()
        listObj2.key = methodKey
        listObj2.readFromDB()

        assertLinesMatch(expectList, listObj2.list)
    }

    @Test
    fun `Test custom RWP`() {
        val methodKey = nextID.toLong()
        val expectUUID = UUID.randomUUID()

        val customRWPObject = CustomUUIDRWP()
        customRWPObject.key = methodKey
        customRWPObject.uuid = expectUUID
        customRWPObject.writeInSingleTX()

        val cObj2 = CustomUUIDRWP()
        cObj2.key = methodKey
        cObj2.readFromDB()

        assertEquals(expectUUID, cObj2.uuid)
    }

    @Test
    fun `Test no RWP fails`() {
        assertThrows<IllegalStateException>("There is no RWP for the type class java.io.ByteArrayOutputStream!") { env.openDbi(NoRWP) }
    }

    @Test
    fun `Test map basics`() {
        val methodKey = nextID.toLong()
        val expectMap = hashMapOf("this" to -1, "is" to 42, "a" to 0, "test" to Int.MAX_VALUE)

        val testObj = MapTester()
        testObj.key = methodKey
        testObj.map = expectMap

        testObj.writeInSingleTX()

        val mapObj2 = MapTester()
        mapObj2.key = methodKey
        mapObj2.readFromDB()

        assertEquals(expectMap, mapObj2.map)
    }

    @Test
    fun `Test get by not key`() {
        val key1 = nextID.toLong()
        val key2 = nextID.toLong()
        val key3 = nextID.toLong()

        val m1 = MisalignedShortArray()
        m1.key = key1
        m1.single = 56.toByte()
        m1.zArray = ShortArray(1)
        m1.writeInSingleTX()
        val m2 = MisalignedShortArray()
        m2.key = key2
        m2.single = 57.toByte()
        m2.zArray = ShortArray(2)
        m2.writeInSingleTX()
        val m3 = MisalignedShortArray()
        m3.key = key3
        m3.single = 58.toByte()
        m3.zArray = ShortArray(3)
        m3.writeInSingleTX()

        val expectM2 = MisalignedShortArray.getElementsWithEquality(MisalignedShortArray::single, 57.toByte())
        assertNotNull(expectM2.firstOrNull())
        assertEquals(1, expectM2.size)
        assertEquals(2, expectM2.first().zArray.size)
    }

    @Test
    fun `Get with custom equality`() {
        val key1 = nextID.toLong()
        val key2 = nextID.toLong()

        val eqCheck = "This is my custom eq check string!"

        val l1 = ListTester()
        l1.key = key1
        l1.list = arrayListOf("Strings!", "Cause why not!", eqCheck)
        l1.writeInSingleTX()
        val l2 = ListTester()
        l2.key = key2
        l2.list = arrayListOf("Strings Extra!", "Cause why not though!", "Another for good measure!")
        l2.writeInSingleTX()

        val obj = ListTester.getElementsWithEqualityFunction(ListTester::list) { it.contains(eqCheck) }.firstOrNull()
        assertNotNull(obj)
        assertEquals(key1, obj!!.key)

        val obj2 = ListTester.getElementsWithEqualityFunction(ListTester::list) { it.contains("Something that doesn't show up") }
        assertEquals(0, obj2.size)
    }

    @Test
    fun `Add find remove no find`() {
        val key1 = nextID
        val dataItem = 12345

        assertEquals(0, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)

        val obj1 = TestObj()
        obj1.key = key1
        obj1.data = 12345
        obj1.writeInSingleTX()

        assertEquals(1, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)

        obj1.delete()

        assertEquals(0, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)
    }
}