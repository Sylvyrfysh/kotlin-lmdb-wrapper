package com.nicholaspjohnson.kotlinlmdbwrapper

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

object NoDBBaseObjectTester {
    @Test
    fun `Test that all stay expected`() {
        val bool = true
        val byte = 23.toByte()
        val short = 652.toShort()
        val char = 'Z'
        val int = 74482
        val float = 2578904.245f
        val long = 31758269743098133L
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

        assertEquals(bool, ato.bool)
        assertEquals(byte, ato.byte)
        assertEquals(short, ato.short)
        assertEquals(char, ato.char)
        assertEquals(int, ato.int)
        assertEquals(float, ato.float)
        assertEquals(long, ato.long)
        assertEquals(double, ato.double)
        assertEquals(varlong, ato.varlong)
        assertEquals(varchar, ato.varchar)
    }
}