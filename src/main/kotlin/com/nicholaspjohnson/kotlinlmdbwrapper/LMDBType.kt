package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer

/**
 * This class represents a type that can be written to the DB used. Some are pre-provided, but you can add your own.
 *
 * @param T The type this object will hold
 * @property clazz The class of T
 * @property align The align required for this type. Must be an integer >=1.
 * @property isConstSize If this type is const-sized. Ints, Bytes. Longs, not strings. If true, align MUST = 1
 * @property maxSize The maximum size of this type.
 * @property minSize The minimum size, if this is a non-const sized object. Must be greater than or equal to align
 */
open class LMDBType<T>(val clazz: Class<T>, val align: Int, val isConstSize: Boolean, val maxSize: Int, val minSize: Int = maxSize) {
    init {
        require(align > 0 && (align and (align - 1)) == 0) { "Align must be a positive power of two!" }
        if (!isConstSize) {
            require(align == 1) { "Items that are not const-sized need an alignment of 1!" }
            require(maxSize > minSize) { "Items that are not const-sized need maxSize > minSize!" }
        } else {
            require(maxSize == minSize) { "Items that are const-sized need maxSize == minSize!" }
        }
        require(minSize >= align) { "minSize must be >= align!" }
    }
    companion object {
        /**
         * A basic [LMDBType] for [Boolean]s
         */
        @JvmField
        val LBool = object: LMDBType<Boolean>(Boolean::class.java, 1, true, 1) {}
        /**
         * A basic [LMDBType] for [Byte]s
         */
        @JvmField
        val LByte = object : LMDBType<Byte>(Byte::class.java, 1, true, 1) {}
        /**
         * A basic [LMDBType] for [Char]s
         */
        @JvmField
        val LChar = object : LMDBType<Char>(Char::class.java, 2, true, 2) {}
        /**
         * A basic [LMDBType] for [Short]s
         */
        @JvmField
        val LShort = object : LMDBType<Short>(Short::class.java, 2, true, 2) {}
        /**
         * A basic [LMDBType] for [Int]s
         */
        @JvmField
        val LInt = object : LMDBType<Int>(Int::class.java, 4, true, 4) {}
        /**
         * A basic [LMDBType] for [Float]s
         */
        @JvmField
        val LFloat = object : LMDBType<Float>(Float::class.java, 4, true, 4) {}
        /**
         * A basic [LMDBType] for [Long]s
         */
        @JvmField
        val LLong = object : LMDBType<Long>(Long::class.java, 8, true, 8) {}
        /**
         * A basic [LMDBType] for [Double]s
         */
        @JvmField
        val LDouble = object : LMDBType<Double>(Double::class.java, 8, true, 8) {}
        /**
         * A basic [LMDBType] for [Long]s that will internally be stored as a VarLong
         */
        @JvmField
        val LVarLong = object : LMDBType<Long>(Long::class.java, 1, false, 10, 1) {
            override fun getItemSizeFromDB(data: ByteBuffer, startPoint: Int): Int {
                var size = 0
                while (data[startPoint + size].toInt() and 0x80 == 0x80) {
                    ++size
                }
                return size + 1
            }

            override fun getItemSizeFromPlain(item: Long) = item.getVarLongSize()
        }

        /**
         * Creates a new [LMDBType] for [String]s of max size [maxSize]
         */
        @JvmStatic
        fun LVarChar(maxSize: Int): LMDBType<String> = object : LMDBType<String>(String::class.java, 1, false, maxSize + (2 * maxSize.toLong().getVarLongSize()), LVarLong.minSize) {
            override fun getItemSizeFromDB(data: ByteBuffer, startPoint: Int): Int {
                val diskSize = data.readVarLong(startPoint)
                if (diskSize == 0L) {
                    return 1
                }
                return diskSize.toInt()
            }

            override fun getItemSizeFromPlain(item: String): Int {
                if (item.isEmpty()) {
                    return 1
                }
                val diskSizeLen = LVarLong.getItemSizeFromPlain(item.length.toLong())
                val dataLenLen = LVarLong.getItemSizeFromPlain(item.length.toLong())
                return diskSizeLen + dataLenLen + item.length
            }
        }
    }

    /**
     * Calculates how much size this item will take in the DB
     *
     * @param item The item to check
     * @return How much size is needed to write this item to a buffer
     */
    open fun getItemSizeFromPlain(item: T): Int {
        if (isConstSize) {
            return maxSize
        }
        error("Variable sized items must override this function!")
    }

    /**
     * Calculates the size that this data takes on-disk.
     * This does not calculate anything like the size of the data in that chunk, which could be less than this size.
     *
     * @param data The buffer this item resides in
     * @param startPoint The point this data starts at
     * @return The size this piece of data takes on-disk
     */
    open fun getItemSizeFromDB(data: ByteBuffer, startPoint: Int): Int {
        if (isConstSize) {
            return maxSize
        }
        error("Variable sized items must override this function!")
    }

    /**
     * Custom check for detecting if we equal [other]
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LMDBType<*>) return false

        if (clazz != other.clazz) return false
        if (align != other.align) return false
        if (isConstSize != other.isConstSize) return false
        if (maxSize != other.maxSize) return false
        if (minSize != other.minSize) return false

        return true
    }

    /**
     * Custom hashCode
     */
    override fun hashCode(): Int {
        var result = clazz.hashCode()
        result = 31 * result + align
        result = 31 * result + isConstSize.hashCode()
        result = 31 * result + maxSize
        result = 31 * result + minSize
        return result
    }
}