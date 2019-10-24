package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import java.nio.ByteBuffer
import kotlin.experimental.and
import kotlin.experimental.or
import kotlin.reflect.KProperty

/**
 * A base delegate for a RWP that will get [index] through [lmdbObject] and [propertyName]
 *
 * @param[lmdbObject] Class of type [M] to operate on
 * @param[propertyName] The property name to give an RWP for
 *
 * @constructor
 * Set up the RWP with the passed in with the underlying object [lmdbObject] and the property name [propertyName]
 *
 */
abstract class AbstractRWP<M: BaseLMDBObject<M>, R>(private val lmdbObject: BaseLMDBObject<M>, private val propertyName: String) : RWPInterface<M> {
    /**
     * Evaluates the index on first get.
     */
    private val index by lazy { lmdbObject.getInd(propertyName) }

    /**
     * Whether or not this RWP can contain a null value
     */
    private val nullable by lazy { lmdbObject.getIsNullable(index) }

    /**
     * Evaluates the index on first get.
     */
    protected var field: R? = null

    /**
     * Returns the size of this object in the database without any nullable headers. Will only be called with non-null mebers.
     */
    protected abstract val getSize: (R) -> Int

    /**
     * Sets the [value] of [property] in object [thisRef] of type [M].
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        field = value as R?
    }

    /**
     * Gets the value of [property] in [thisRef] of type [M].
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> getValue(thisRef: M, property: KProperty<*>): T {
        return field as T
    }

    /**
     * Writes to [writeBuffer] at [startingOffset] and returns how many bytes were written.
     */
    abstract fun writeToDB(writeBuffer: ByteBuffer, startingOffset: Int): Int

    /**
     * Reads from [readBuffer] at [startingOffset] and returns how many bytes were read.
     */
    abstract fun readFromDB(readBuffer: ByteBuffer, startingOffset: Int): Int

    /**
     * Internal helper method for writing an object. If the object is nullable, it will write a nullable header at [startingOffset].
     * It will call the specified internal write function with the correct [startingOffset] if the backing object is not null.
     * Returns the number of bytes written.
     */
    protected fun write(writeBuffer: ByteBuffer, startingOffset: Int, writeFunc: (Int) -> Int): Int {
        if (nullable) {
            writeNullableHeader(writeBuffer, startingOffset)
            if (field != null) {
                return 1 + writeFunc(startingOffset + 1)
            }
            return 1
        } else {
            return writeFunc(startingOffset)
        }
    }

    /**
     * Internal helper method for reading an object. If the object is nullable, it will read the nullable header at [startingOffset].
     * It will call the specified internal read function with the correct [startingOffset] if the backing object is not null.
     * Returns the number of bytes read.
     */
    protected fun read(readBuffer: ByteBuffer, startingOffset: Int, readFunc: (Int) -> Int): Int {
        if (nullable) {
            val isNull = readNullableHeader(readBuffer, startingOffset)
            if (isNull) {
                return 1
            }
            return 1 + readFunc(startingOffset + 1)
        }
        return readFunc(startingOffset)
    }

    /**
     * Returns the number of bytes this object will be in DB.
     */
    override fun getDiskSize(): Int {
        return if (nullable) {
            return if (field == null) {
                1
            } else {
                1 + getSize(field!!)
            }
        } else {
            getSize(field!!)
        }
    }

    /**
     * Writes a single byte that marks whether this object is null.
     *
     * If [field] is null, will be the byte 1, else 0.
     */
    private fun writeNullableHeader(writeBuffer: ByteBuffer, startingOffset: Int) {
        var nullableInfoByte: Byte = 0
        if (field == null) {
            nullableInfoByte = nullableInfoByte or NULLABLE_NULL_BIT
        }
        writeBuffer.put(startingOffset, nullableInfoByte)
    }

    /**
     * Reads a single byte that tells whether this object is null and/or compacted.
     * Returns a [Pair] of booleans that are (null, compacted). If null is false, compacted will be false.
     * If compacted is true, there is no more data for this object after this byte.
     */
    private fun readNullableHeader(readBuffer: ByteBuffer, startingOffset: Int): Boolean {
        val nullableInfoByte: Byte = readBuffer[startingOffset]
        return (nullableInfoByte and NULLABLE_NULL_BIT) == NULLABLE_NULL_BIT
    }

    /**
     * Utility pieces
     */
    companion object {
        /**
         * The bit to set in the nullable header if this item is null.
         */
        private const val NULLABLE_NULL_BIT = (1 shl 0).toByte()
    }
}