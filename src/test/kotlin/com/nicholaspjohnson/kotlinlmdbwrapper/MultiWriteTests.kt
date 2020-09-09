package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.LMDBEnv
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.nio.file.Files
import java.nio.file.Paths

object MultiWriteTests {
    private val isCI = System.getenv("CI") != null

    private lateinit var env: LMDBEnv

    private val tenItems = Array(10) {
        MultiWrite(it.toLong(), 100L + (it * 10))
    }

    private val tenLaterItems = Array(10) {
        MultiWrite(10L + it.toLong(), 100L + (it * 10))
    }

    @BeforeAll
    @JvmStatic
    fun `Set Up`() {
        Assumptions.assumeTrue(!isCI, "Cannot run in a CI")

        if (Files.exists(Paths.get("db"))) {
            Files.list(Paths.get("db")).forEach(Files::delete)
        } else {
            Files.createDirectories(Paths.get("db"))
        }

        env = LMDBEnv(Paths.get("db"), numDbis = 32)
        env.openDbi(MultiWrite)
    }

    @BeforeEach
    fun `Clean DB`() {
        MultiWrite.deleteAllEntries()
    }

    @AfterAll
    @JvmStatic
    fun `Tear Down`() {
        Assumptions.assumeTrue(!isCI)

        env.close()
    }

    @Test
    fun `Test write multiple`() {
        assertEquals(0, MultiWrite.getNumberOfEntries())

        MultiWrite.writeMultiple(tenItems)
        assertEquals(10, MultiWrite.getNumberOfEntries())

        MultiWrite.deleteAllEntries()
        assertEquals(0, MultiWrite.getNumberOfEntries())
    }

    @Test
    fun `Find key range`() {
        MultiWrite.writeMultiple(tenItems)

        assertEquals(5, MultiWrite.getElementsByKeyRange(0L, 5L).size)
        assertEquals(6, MultiWrite.getElementsByKeyRange(0L, 5L, endInclusive = true).size)

        assertEquals(1, MultiWrite.getElementsWithEquality(MultiWrite::data, 110).size)
        assertEquals(0, MultiWrite.getElementsWithEquality(MultiWrite::data, 111).size)

        assertEquals(
            3,
            MultiWrite.getElementsWithMemberEqualityFunction(MultiWrite::data) { it >= 170 }.size
        )
    }

    @Test
    fun `Get each with limit`() {
        MultiWrite.writeMultiple(tenItems)

        val items = MultiWrite.getEach(5L)
        assertEquals(5, items.size)
    }

    @Test
    fun `Get each after later in list`() {
        MultiWrite.writeMultiple(tenItems)

        val items = MultiWrite.getEach(after = 6L)
        assertEquals(3, items.size)
    }

    @Test
    fun `Get each later in list`() {
        MultiWrite.writeMultiple(tenItems)

        val items = MultiWrite.getEach(5L, after = 2L)
        assertEquals(5, items.size)
        assertEquals(3L, items.first().key)
        assertEquals(7L, items.last().key)
    }

    @Test
    fun `Get with equality`() {
        MultiWrite.writeMultiple(tenItems)

        assertEquals(
            3,
            MultiWrite.getElementsWithEqualityFunction(after = 4L, limit = 3L) { it.data and 2L != 0L }.size
        )
    }

    @Test
    fun `Get each with after before all`() {
        MultiWrite.writeMultiple(tenLaterItems)

        assertEquals(10, MultiWrite.getEach(after = 2L).size)
    }
}