package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize

import com.nicholaspjohnson.kotlinlmdbwrapper.BaseLMDBObject
import org.lwjgl.system.MemoryUtil
import java.nio.ByteBuffer
import kotlin.reflect.KProperty
import kotlin.text.Charsets.UTF_8

/**
 * A default [String] RWP that will act on instances of the class [M] with a [maximumLength].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [propertyName] to the underlying [VarSizeRWP], and holds [maximumLength] for the maximum length of this string.
 */
class VarCharRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, name: String, private val maximumLength: Int) : VarSizeRWP<M, String?>(obj, name) {
    override val readFn: (ByteBuffer, Int) -> String? =
        Companion::compReadFn
    override val writeFn: (ByteBuffer, Int, String?) -> Any? =
        Companion::compWriteFn
    override val getItemOnlySize: (String?) -> Int =
        Companion::compSizeFn

    @Suppress("UNCHECKED_CAST")
    override fun <T> setValue(thisRef: M, property: KProperty<*>, value: T) {
        val temp = value as String?
        if (temp != null) {
            require(temp.length <= maximumLength) { "Strings cannot be longer than their maximum length! (max $maximumLength, attempt ${temp.length})" }
        }
        field = temp
    }

    /**
     * Helper methods.
     */
    companion object {
        /**
         * Returns the raw size of [item] when encoded in UTF-8.
         */
        private fun compSizeFn(item: String?): Int {
            return MemoryUtil.memLengthUTF8(item!!, false)
        }

        /**
         * Writes non-null [value] to [buffer] at [offset].
         */
        private fun compWriteFn(buffer: ByteBuffer, offset: Int, value: String?) {
            if (buffer.isDirect) {
                MemoryUtil.memUTF8(value!!, false, buffer, offset)
            } else {
                buffer.position(offset)
                buffer.put(UTF_8.encode(value!!))
                buffer.position(0)
            }
        }

        /**
         * Reads and returns the non-null value in [buffer] at [offset].
         */
        private fun compReadFn(buffer: ByteBuffer, offset: Int): String {
            return if (buffer.isDirect) {
                MemoryUtil.memUTF8(buffer)
            } else {
                UTF_8.decode(buffer).toString()
            }
        }
    }
}
