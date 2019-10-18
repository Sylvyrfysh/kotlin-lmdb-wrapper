package com.nicholaspjohnson.kotlinlmdbwrapper

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LMDBTypeTester {
    @Test
    fun `Non-const size max gt min`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, false, 1, 2)
        }
        assertEquals("Items that are not const-sized need maxSize > minSize!", except.message)
    }

    @Test
    fun `Const size max eq min`() {
        val except = assertThrows<IllegalArgumentException> {
            LMDBType(Any::class.java, true, 2, 1)
        }
        assertEquals("Items that are const-sized need maxSize == minSize!", except.message)
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
        assertEquals(LMDBType(Byte::class.java, true, 1, 1), LMDBType(Byte::class.java, true, 1, 1))
    }

    @Test
    fun `Test Custom diff T not equal`() {
        assertNotEquals(LMDBType(Byte::class.java, true, 1, 1), LMDBType(Char::class.java, true, 1, 1))
    }
}