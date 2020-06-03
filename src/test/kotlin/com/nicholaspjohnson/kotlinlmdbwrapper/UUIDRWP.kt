package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.constsize.ConstSizeRWP
import java.nio.ByteBuffer
import java.util.*

class UUIDRWP<M: LMDBObject<M>>(obj: LMDBObject<M>, nullable: Boolean) : ConstSizeRWP<M, UUID?>(obj, nullable) {
    override val itemSize: Int = Long.SIZE_BYTES + Long.SIZE_BYTES
    override val readFn: (ByteBuffer, Int) -> UUID = { buffer, off ->
        UUID(buffer.getLong(off), buffer.getLong(off + Long.SIZE_BYTES))
    }
    override val writeFn: (ByteBuffer, Int, UUID?) -> Any? = { buffer, off, item ->
        buffer.putLong(off, item!!.mostSignificantBits)
        buffer.putLong(off + Long.SIZE_BYTES, item.leastSignificantBits)
    }
}