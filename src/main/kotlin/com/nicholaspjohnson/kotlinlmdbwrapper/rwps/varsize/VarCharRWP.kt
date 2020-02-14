package com.nicholaspjohnson.kotlinlmdbwrapper.rwps.varsize

import com.nicholaspjohnson.kotlinlmdbwrapper.LMDBObject
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.RWPCompanion
import org.lwjgl.system.MemoryUtil
import java.nio.ByteBuffer
import kotlin.reflect.KProperty
import kotlin.text.Charsets.UTF_8

/**
 * A default [String] RWP that will act on instances of the class [M].
 *
 * @constructor
 *
 * Passes [lmdbObject] and [nullable] to the underlying [VarSizeRWP].
 */
class VarCharRWP<M: LMDBObject<M>>(obj: LMDBObject<M>, nullable: Boolean) : VarSizeRWP<M, String?>(obj, nullable) {
    override val readFn: (ByteBuffer, Int) -> String? =
        Companion::compReadFn
    override val writeFn: (ByteBuffer, Int, String?) -> Any? =
        Companion::compWriteFn
    override val getItemOnlySize: (String?) -> Int =
        Companion::compSizeFn

    @Suppress("UNCHECKED_CAST")
    override fun <T> setValue(thisRef: LMDBObject<M>, property: KProperty<*>, value: T) {
        val temp = value as String?
        field = temp
    }

    /**
     * Helper methods.
     */
    companion object: RWPCompanion<VarCharRWP<*>, String?> {
        /**
         * Returns the raw size of [item] when encoded in UTF-8.
         */
        override fun compSizeFn(item: String?): Int {
            return MemoryUtil.memLengthUTF8(item!!, false)
        }

        /**
         * Writes non-null [item] to [buffer] at [offset].
         */
        override fun compWriteFn(buffer: ByteBuffer, offset: Int, item: String?) {
            if (buffer.isDirect) {
                MemoryUtil.memUTF8(item!!, false, buffer, offset)
            } else {
                buffer.position(offset)
                buffer.put(UTF_8.encode(item!!))
                buffer.position(0)
            }
        }

        /**
         * Reads and returns the non-null value in [buffer] at [offset].
         */
        override fun compReadFn(buffer: ByteBuffer, offset: Int): String {
            return if (buffer.isDirect) {
                MemoryUtil.memUTF8(buffer)
            } else {
                UTF_8.decode(buffer).toString()
            }
        }
    }
}
