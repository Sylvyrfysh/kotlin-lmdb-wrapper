package com.nicholaspjohnson.kotlinlmdbwrapper

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class ExtensionFunctionsKtTest {
    @Test
    fun `Test VarLong read 127`() {
        val buffer = ByteBuffer.wrap(byteArrayOf(0x7F.toByte()))
        assertEquals(127, buffer.readVarLong(0))
    }

    @Test
    fun `Test VarLong read 128`() {
        val buffer = ByteBuffer.wrap(byteArrayOf(0x80.toByte(), 0x01.toByte()))
        assertEquals(128, buffer.readVarLong(0))
    }

    @Test
    fun `Test VarLong read 143`() {
        val buffer = ByteBuffer.wrap(byteArrayOf(0x8F.toByte(), 0x01.toByte()))
        assertEquals(143, buffer.readVarLong(0))
    }

    @Test
    fun `Test VarLong write 127`() {
        val arr127 = byteArrayOf(127.toByte())
        val read127 = ByteArray(1)
        val buff = ByteBuffer.allocate(1)
        buff.writeVarLong(0, 127L)
        buff.get(read127)
        assertArrayEquals(arr127, read127)
    }

    @Test
    fun `Test VarLong write 128`() {
        val arr128 = byteArrayOf(0b10000000.toByte(), 0b00000001)
        val read128 = ByteArray(2)
        val buff = ByteBuffer.allocate(2)
        buff.writeVarLong(0, 128L)
        buff.get(read128)
        assertArrayEquals(arr128, read128)
    }

    @Test
    fun `Test VarLong write 143`() {
        val arr143 = byteArrayOf(0b10001111.toByte(), 0b00000001)
        val read143 = ByteArray(2)
        val buff = ByteBuffer.allocate(2)
        buff.writeVarLong(0, 143L)
        buff.get(read143)
        assertArrayEquals(arr143, read143)
    }

    @Test
    fun `Test VarLong Size of 0`() {
        assertEquals(1, 0L.getVarLongSize())
    }

    @Test
    fun `Test VarLong Size of 127`() {
        assertEquals(1, 127L.getVarLongSize())
    }

    @Test
    fun `Test VarLong Size of 128`() {
        assertEquals(2, 128L.getVarLongSize())
    }

    @Test
    fun `Test VarLong Size of Long MAX`() {
        assertEquals(9, Long.MAX_VALUE.getVarLongSize())
    }

    @Test
    fun `Test VarLong Size of Long MIN`() {
        assertEquals(10, Long.MIN_VALUE.getVarLongSize())
    }
}