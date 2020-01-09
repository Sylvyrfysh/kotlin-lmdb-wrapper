package com.nicholaspjohnson.kotlinlmdbwrapper.rwps

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.lmdb.NullStoreOption
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import java.nio.ByteBuffer
import kotlin.experimental.and
import kotlin.experimental.or
import kotlin.reflect.KProperty

/**
 * An RWP that writes to DB for type [R] that is [nullable].
 *
 * @param[lmdbObject] Class of type [M] to operate on
 *
 * @constructor
 * Set up the RWP with the passed in with the underlying object [lmdbObject] that is [nullable].
 *
 */
abstract class AbstractRWP<M: BaseLMDBObject<M>, R>(private val lmdbObject: BaseLMDBObject<M>, private val nullable: Boolean) : RWPInterface<M> {
    /**
     * Backing RWP for this field
     */
    protected var field: R? = null

    /**
     * Returns the size of this object in the database without any nullable headers. Will only be called with non-null members.
     */
    internal abstract val getSize: (R) -> Int

    /**
     * Sets the [value] of [property] in object [thisRef] of type [M].
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> setValue(thisRef: BaseLMDBObject<M>, property: KProperty<*>, value: T) {
        field = value as R?
    }

    /**
     * Gets the value of [property] in [thisRef] of type [M].
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T> getValue(thisRef: BaseLMDBObject<M>, property: KProperty<*>): T {
        lmdbObject.setUsed()
        return field as T
    }

    /**
     * Writes to [writeBuffer] at [startingOffset] and returns how many bytes were written.
     */
    abstract fun writeToDB(
        writeBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int

    /**
     * Reads from [readBuffer] at [startingOffset] and returns how many bytes were read.
     */
    abstract fun readFromDB(
        readBuffer: ByteBuffer,
        startingOffset: Int,
        nullStoreOption: NullStoreOption
    ): Int

    /**
     * Internal helper method for writing an object. If the object is nullable, it will write a nullable header at [startingOffset].
     * It will call the specified internal write function with the correct [startingOffset] if the backing object is not null.
     * Returns the number of bytes written.
     */
    protected fun write(writeBuffer: ByteBuffer, startingOffset: Int, nullStoreOption: NullStoreOption, writeFunc: (Int) -> Int): Int {
        if (nullable) {
            writeNullableHeader(writeBuffer, startingOffset)
            if (field != null) {
                return 1 + writeFunc(startingOffset + 1)
            }
            return 1 + if (nullStoreOption == NullStoreOption.SPEED && this is ConstSizeRWP<*, *>) (this as ConstSizeRWP<*, *>).itemSize else 0
        } else {
            return writeFunc(startingOffset)
        }
    }

    /**
     * Internal helper method for reading an object. If the object is nullable, it will read the nullable header at [startingOffset].
     * It will call the specified internal read function with the correct [startingOffset] if the backing object is not null.
     * Returns the number of bytes read.
     */
    protected fun read(readBuffer: ByteBuffer, startingOffset: Int, nullStoreOption: NullStoreOption, readFunc: (Int) -> Int): Int {
        if (nullable) {
            val isNull = readNullableHeader(readBuffer, startingOffset)
            if (isNull) {
                return 1 + if (nullStoreOption == NullStoreOption.SPEED && this is ConstSizeRWP<*, *>) (this as ConstSizeRWP<*, *>).itemSize else 0
            }
            return 1 + readFunc(startingOffset + 1)
        }
        return readFunc(startingOffset)
    }

    private var size = 0
    private var sizeIsCalculated = false

    /**
     * Returns the number of bytes this object will be in DB.
     */
    override fun getDiskSize(nullStoreOption: NullStoreOption): Int {
        if (sizeIsCalculated) {
            return size
        }

        val localSize = when {
            nullable -> {
                if (field == null) {
                    1 + if (nullStoreOption == NullStoreOption.SPEED && this is ConstSizeRWP<*, *>) itemSize else 0
                } else {
                    1 + getSize(field!!)
                }
            }
            this is ConstSizeRWP<M, *> -> itemSize
            else -> getSize(field!!)
        }

        if ((!nullable || nullStoreOption == NullStoreOption.SPEED) && this is ConstSizeRWP<*, *>) {
            size = localSize
            sizeIsCalculated = true
        }

        return localSize
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
     * Utility pieces
     */
    companion object {
        /**
         * The bit to set in the nullable header if this item is null.
         */
        private const val NULLABLE_NULL_BIT = (1 shl 0).toByte()

        /**
         * Reads a single byte that tells whether this object is null and/or compacted.
         * Returns a [Pair] of booleans that are (null, compacted). If null is false, compacted will be false.
         * If compacted is true, there is no more data for this object after this byte.
         */
        fun readNullableHeader(readBuffer: ByteBuffer, startingOffset: Int): Boolean {
            val nullableInfoByte: Byte = readBuffer[startingOffset]
            return (nullableInfoByte and NULLABLE_NULL_BIT) == NULLABLE_NULL_BIT
        }
    }
}