package com.nicholaspjohnson.kotlinlmdbwrapper

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.rules.ExpectedException
import org.junit.Rule

class LMDBTypeTester {
    @get:Rule
    var thrown: ExpectedException = ExpectedException.none()

    @Test
    fun `Align greater than 0`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("Align must be a positive power of two!")

        LMDBType(Any::class.java, 0, true, 1)
    }

    @Test
    fun `Align power of 2`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("Align must be a positive power of two!")

        LMDBType(Any::class.java, 3, true, 1)
    }

    @Test
    fun `Non-const size needs align 1`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("Items that are not const-sized need an alignment of 1!")

        LMDBType(Any::class.java, 2, false, 1)
    }

    @Test
    fun `Non-const size max gt min`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("Items that are not const-sized need maxSize > minSize!")

        LMDBType(Any::class.java, 1, false, 1, 2)
    }

    @Test
    fun `Const size max eq min`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("Items that are const-sized need maxSize == minSize!")

        LMDBType(Any::class.java, 1, true, 2, 1)
    }

    @Test
    fun `minSize gte align`() {
        thrown.expect(IllegalArgumentException::class.java)
        thrown.expectMessage("minSize must be >= align!")

        LMDBType(Any::class.java, 4, true, 1, 1)
    }

    @Test
    fun `Test LLong equals LLong`() {
        assertEquals(LMDBType.LLong, LMDBType.LLong)
    }

    @Test
    fun `Test LLong not equals LDouble`() {
        assertNotEquals(LMDBType.LLong, LMDBType.LDouble)
    }

    @Test
    fun `Test LLong not equals LVarLong`() {
        assertNotEquals(LMDBType.LLong, LMDBType.LVarLong)
    }

    @Test
    fun `Test Custom types equal`() {
        assertEquals(LMDBType(Byte::class.java, 1, true, 1, 1), LMDBType(Byte::class.java, 1, true, 1, 1))
    }

    @Test
    fun `Test Custom diff T not equal`() {
        assertNotEquals(LMDBType(Byte::class.java, 1, true, 1, 1), LMDBType(Char::class.java, 1, true, 1, 1))
    }
}