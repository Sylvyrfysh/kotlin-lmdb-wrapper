package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.DataNotFoundException
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBEnv
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Files
import java.nio.file.Paths

object BasicDBTester {
    private val isCI = System.getenv("CI") != null

    private lateinit var env: LMDBEnv
    private val testObj1 = TestObj(1, 1234)

    private var nextID: Int = 2
        get() = (field++)

    @BeforeAll
    @JvmStatic
    fun `Set Up`() {
        assumeTrue(!isCI, "Cannot run in a CI")

        if (Files.exists(Paths.get("db"))) {
            Files.list(Paths.get("db")).forEach(Files::delete)
        } else {
            Files.createDirectories(Paths.get("db"))
        }

        env = LMDBEnv(Paths.get("db"), numDbis = 32)
        env.openDbi(TestObj)
        env.openDbi(MixNormalNulls)
        env.openDbi(AllTypesObject)
        env.openDbi(MultiWrite)
        env.openDbi(ListTester)

        testObj1.write()
    }
    
    @AfterAll
    @JvmStatic
    fun `Tear Down`() {
        assumeTrue(!isCI)

        env.close()
    }

    @Test
    fun `Test Basic Read Write`() {
        val testObj2 = TestObj.read(1)
        assertEquals(testObj1.data, testObj2.data)
    }

    @Test
    fun `Test NonExistent Key throws Exception`() {
        assertThrows<DataNotFoundException>("The key supplied does not have any data in the DB!") {
            TestObj.read(Integer.MIN_VALUE)
        }
    }

    @Test
    fun `Test ReWrite Does not Modify Others Until ReRead`() {
        val methodKey = nextID
        val firstData = 5678
        val testObj2 = TestObj(methodKey, firstData)
        testObj2.write()
        val testObj3 = TestObj.read(methodKey)
        assertEquals(firstData, testObj3.data)
        testObj2.data = 9012
        testObj2.write()
        assertEquals(firstData, testObj3.data)
        val testObj4 = TestObj.read(methodKey)
        assertEquals(9012, testObj4.data)
    }

    @Test
    fun `Test data stays null`() {
        val methodKey = nextID
        val testObj2 = TestObj(methodKey, null)
        testObj2.write()
        val testObj3 = TestObj.read(methodKey)
        assertEquals(null, testObj3.data)
    }

    @Test
    fun `Test data offsets after load`() {
        val methodKey = nextID
        val mixNormalNulls = MixNormalNulls(methodKey, 35, null, "This is something I guess")
        mixNormalNulls.write()

        val mixNormalNulls2 = MixNormalNulls.read(methodKey)

        assertEquals(methodKey, mixNormalNulls2.key)
        assertNotNull(mixNormalNulls2.nullableInt)
        assertEquals(35, mixNormalNulls2.nullableInt!!)
        assertEquals("This is something I guess", mixNormalNulls2.normalString)
        assertNull(mixNormalNulls2.aNullableString)
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
        val varchar = "This is a testing string. It isn't that special."
        val nullableInt: Int? = 898

        val ato = AllTypesObject(bool, byte, short, char, int, float, long, double, varchar, nullableInt)
        ato.write()

        val ato2 = AllTypesObject.read(methodKey)

        assertEquals(bool, ato2.bool)
        assertEquals(byte, ato2.byte)
        assertEquals(short, ato2.short)
        assertEquals(char, ato2.char)
        assertEquals(int, ato2.int)
        assertEquals(float, ato2.float)
        assertEquals(long, ato2.key)
        assertEquals(double, ato2.double)
        assertEquals(varchar, ato2.varchar)
        assertEquals(nullableInt, ato2.nullableInt)

        assertEquals(1, AllTypesObject.getElementsWithEquality(AllTypesObject::bool, ato.bool).size)
        assertEquals(1, AllTypesObject.getElementsWithEquality(AllTypesObject::nullableInt, ato.nullableInt).size)
        assertEquals(0, AllTypesObject.getElementsWithEquality(AllTypesObject::nullableInt, null).size)
    }

    @Test
    fun `Test list basics`() {
        val methodKey = nextID.toLong()
        val expectList = arrayListOf("Why", "like", "This? IDK")

        val listObj = ListTester(methodKey, expectList)
        listObj.write()

        val listObj2 = ListTester.read(methodKey)
        assertLinesMatch(expectList, listObj2.list)
    }

    @Test
    fun `Get with custom equality`() {
        val key1 = nextID.toLong()
        val key2 = nextID.toLong()

        val eqCheck = "This is my custom eq check string!"

        val l1 = ListTester(key1, arrayListOf("Strings!", "Cause why not!", eqCheck))
        l1.write()
        val l2 = ListTester(key2, arrayListOf("Strings Extra!", "Cause why not though!", "Another for good measure!"))
        l2.write()

        val obj = ListTester.getElementsWithMemberEqualityFunction(ListTester::list) { it.contains(eqCheck) }.firstOrNull()
        assertNotNull(obj)
        assertEquals(key1, obj!!.key)

        val obj2 = ListTester.getElementsWithMemberEqualityFunction(ListTester::list) { it.contains("Something that doesn't show up") }
        assertEquals(0, obj2.size)
    }

    @Test
    fun `Add find remove no find`() {
        val key1 = nextID
        val dataItem = 12345

        assertEquals(0, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)

        val obj1 = TestObj(key1, dataItem)
        obj1.write()

        assertEquals(1, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)

        obj1.delete()

        assertEquals(0, TestObj.getElementsWithEquality(TestObj::data, dataItem).size)
    }
}