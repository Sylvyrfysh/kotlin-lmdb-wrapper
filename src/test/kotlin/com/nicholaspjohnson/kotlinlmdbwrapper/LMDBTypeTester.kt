package com.nicholaspjohnson.kotlinlmdbwrapper

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LMDBTypeTester {
    @Test
    fun `Align greater than 0`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 0, true, 1)
        }
        assertEquals("Align must be a positive power of two!", except.message)
    }

    @Test
    fun `Align power of 2`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 3, true, 1)
        }
        assertEquals("Align must be a positive power of two!", except.message)
    }

    @Test
    fun `Non-const size needs align 1`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 2, false, 1)
        }
        assertEquals("Items that are not const-sized need an alignment of 1!", except.message)
    }

    @Test
    fun `Non-const size max gt min`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 1, false, 1, 2)
        }
        assertEquals("Items that are not const-sized need maxSize > minSize!", except.message)
    }

    @Test
    fun `Const size max eq min`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 1, true, 2, 1)
        }
        assertEquals("Items that are const-sized need maxSize == minSize!", except.message)
    }

    @Test
    fun `minSize gte align`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, 4, true, 1, 1)
        }
        assertEquals("minSize must be >= align!", except.message)
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