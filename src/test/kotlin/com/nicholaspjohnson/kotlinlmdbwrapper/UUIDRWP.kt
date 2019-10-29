package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import java.nio.ByteBuffer
import java.util.*

class UUIDRWP<M: BaseLMDBObject<M>>(obj: BaseLMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, UUID?>(obj, nullable) {
    override val itemSize: Int = 16
    override val readFn: (ByteBuffer, Int) -> UUID = { buffer, off ->
        UUID(buffer.getLong(off), buffer.getLong(off + 8))
    }
    override val writeFn: (ByteBuffer, Int, UUID?) -> Any? = { buffer, off, item ->
        buffer.putLong(off, item!!.mostSignificantBits)
        buffer.putLong(off + 8, item.leastSignificantBits)
    }
}