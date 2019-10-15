package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.*
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryStack.stackPush
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.io.Serializable
import java.nio.*
import kotlin.properties.ReadWriteProperty

private const val SIZE_MARKER_SIZE = Int.SIZE_BYTES

abstract class BaseLMDBObject<M : BaseLMDBObject<M>>(baseTypes: Array<LMDBType<*>>, from: ObjectBufferType) {
    private lateinit var data: ByteBuffer
    private lateinit var dataShorts: ShortBuffer
    private lateinit var dataChars: CharBuffer
    private lateinit var dataInts: IntBuffer
    private lateinit var dataLongs: LongBuffer
    private lateinit var dataFloats: FloatBuffer
    private lateinit var dataDoubles: DoubleBuffer

    private lateinit var types: Array<LMDBType<*>>
    private lateinit var varSizeTypes: Array<LMDBType<*>?>

    private lateinit var offsets: IntArray
    private lateinit var constOffsets: IntArray
    private lateinit var sizes: IntArray
    private lateinit var constSizes: IntArray

    var minBufferSize: Int = -1
        private set
    var maxBufferSize: Int = -1
        private set
    private var constSizeSetSize: Int = -1
    private var maxAlign: Int = -1

    /**
     * True if this object has been committed to the DB, or read from the DB.
     */
    var committed: Boolean = false
        private set
    private var isOnDBAddress: Boolean

    private val preferredOrder = LMDBType.run { listOf(LLong, LDouble, LInt, LFloat, LShort, LChar, LByte, LBool) }

    init {
        setTypes(baseTypes)
        when (from) {
            is ObjectBufferType.New -> {
                initBuffers(ByteBuffer.allocate(maxBufferSize).order(ByteOrder.nativeOrder()))
                dataInts.put(0, maxBufferSize - SIZE_MARKER_SIZE)
                isOnDBAddress = false
            }
            is ObjectBufferType.Buffer -> {
                initBuffers(from.buffer)
                isOnDBAddress = false
            }
            is ObjectBufferType.DBRead -> {
                initBuffers(from.buffer)
                isOnDBAddress = true
                committed = true
                TODO()
            }
        }
    }

    private fun initBuffers(newData: ByteBuffer) {
        require(newData.capacity() >= minBufferSize)
        require(newData.order() == ByteOrder.nativeOrder())
        data = newData
        data.position(SIZE_MARKER_SIZE)
        dataShorts = data.asShortBuffer()
        dataChars = data.asCharBuffer()
        dataInts = data.asIntBuffer()
        dataLongs = data.asLongBuffer()
        dataFloats = data.asFloatBuffer()
        dataDoubles = data.asDoubleBuffer()
        data.position(0)
    }

    private fun setTypes(types: Array<LMDBType<*>>) {
        require(types.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        this.types = types
        minBufferSize = types.map(LMDBType<*>::minSize).sum() + SIZE_MARKER_SIZE //data plus front size marker
        maxBufferSize = types.map(LMDBType<*>::maxSize).sum() + SIZE_MARKER_SIZE //data plus front size marker
        offsets = IntArray(types.size) { -1 }
        sizes = IntArray(types.size) { -1 }
        constOffsets = IntArray(types.size) { -1 }
        constSizes = IntArray(types.size) { -1 }
        calculateConstOffsets()
    }

    private fun calculateConstOffsets() {
        val remainingTypes = Array<LMDBType<*>?>(types.size) { types[it] }

        val cPrefIter = preferredOrder.filter { it in remainingTypes }.iterator()

        var byteOffset = 0 // for the size marker, we need an offset
        while (remainingTypes.asList().filterNotNull().count() > 1) {
            if (cPrefIter.hasNext()) {
                val cPref = cPrefIter.next()
                for ((index, t) in remainingTypes.withIndex()) {
                    if (t == null) {
                        continue
                    }
                    if (t == cPref) {
                        // We only have const-sized types in this loop, so we can use align as size and get offsets that way
                        offsets[index] = byteOffset / t.align
                        constOffsets[index] = byteOffset / t.align
                        sizes[index] = t.align
                        constSizes[index] = t.align
                        remainingTypes[index] = null
                        byteOffset += t.align
                        constSizeSetSize += t.align

                        maxAlign = maxAlign.coerceAtLeast(t.align)
                    }
                }
            } else {
                for ((index, t) in remainingTypes.withIndex()) {
                    if (t == null) {
                        continue
                    }

                    if (t.isConstSize) {
                        // We only have const-sized types in this loop, so we can use align as size and get offsets that way
                        offsets[index] = byteOffset / t.align
                        constOffsets[index] = byteOffset / t.align
                        sizes[index] = t.align
                        constSizes[index] = t.align
                        remainingTypes[index] = null
                        byteOffset += t.align
                        constSizeSetSize += t.align

                        maxAlign = maxAlign.coerceAtLeast(t.align)
                    }
                }
                //non-const sized things will fall here
                for ((index, t) in remainingTypes.withIndex()) {
                    if (t == null) {
                        continue
                    }

                    offsets[index] = byteOffset
                    sizes[index] = t.minSize
                    remainingTypes[index] = null
                    byteOffset += t.minSize
                }
            }
        }

        varSizeTypes = remainingTypes
        println(offsets.contentToString())
    }

    private fun calculateVarSizeOffsets(changedIdx: Int, newSize: Int) {
        if (sizes[changedIdx] >= newSize) {
            return
        }
        val remainingTypes = varSizeTypes.copyOf()

        var needsChange = false
        val newOffsets = offsets.copyOf()

        while (remainingTypes.asList().filterNotNull().count() > 1) {
            for ((index, t) in remainingTypes.withIndex()) {
                if (t == null) {
                    continue
                }

                if (needsChange) {
                    newOffsets[index] += (newSize - sizes[changedIdx])
                    remainingTypes[index] = null
                } else if (index == changedIdx) {
                    needsChange = true
                }
            }
        }

        if (newOffsets.isNotEmpty()) {
            val newBuffer = data.capacity() < ((newSize - sizes[changedIdx]) + sizes.sum())
            val writeBuf =
                if (newBuffer) ByteBuffer.allocate((newSize - sizes[changedIdx]) + sizes.sum()).put(data).position(0) else data
            for (i in newOffsets.toList().withIndex().filterNot { it.value == offsets[it.index] }.sortedByDescending { it.value }) {
                val dCpy = ByteArray(sizes[i.index])
                data.position(offsets[i.index]).get(dCpy)
                writeBuf.position(i.value).put(dCpy)
                TODO("Think it works, check it over though")
            }
            if (newBuffer) {
                initBuffers(writeBuf)
            }
            offsets = newOffsets
        }
        sizes[changedIdx] = newSize
    }

    fun getAllAsByteArray(): ByteArray {
        val retArr = ByteArray(data.capacity())
        data.position(0).get(retArr)
        return retArr
    }

    /**
     * TODO
     *
     * @param T The type of the variable
     * @param index The index of the type passed in the constructor
     * @param type The LMDBType of the variable
     * @return A ReadWriteProperty that will properly delegate the object
     */
    protected fun <T> db(index: Int, type: LMDBType<T>): ReadWriteProperty<M, T> {
        return when (type) {
            LMDBType.LByte -> error("") //ByteRWP(index)
            LMDBType.LBool -> BoolRWP(index)
            LMDBType.LShort -> ShortRWP(index)
            LMDBType.LChar -> CharRWP(index)
            LMDBType.LInt -> IntRWP(index)
            LMDBType.LFloat -> FloatRWP(index)
            LMDBType.LLong -> LongRWP(index)
            LMDBType.LDouble -> DoubleRWP(index)
            else -> error("Type $type is not supported yet!")
        }
    }

    protected fun <T : Serializable> db(index: Int, type: T): ReadWriteProperty<M, T> {
        TODO()
        // TODO: Serialize to BAOS, them copy in
        /*return when (type) {
            LMDBType.LByte -> ByteRWP(index)
            LMDBType.LShort -> ShortRWP(index)
            else -> error("Type $type is not supported yet!")
        }*/
    }

    protected fun <T, R> db(
        index: Int,
        type: LMDBType<T>,
        convertTo: (T) -> R,
        convertFrom: (R) -> T
    ): ReadWriteProperty<M, R> {
        return when (type) {
            LMDBType.LByte -> ByteTypedRWP(index, convertTo, convertFrom)
            LMDBType.LShort -> ShortTypedRWP(index, convertTo, convertFrom)
            else -> error("Type $type with conversions is not supported yet!")
        }
    }

    abstract fun keyFunc(stack: MemoryStack): ByteBuffer

    fun writeInSingleTX(env: Long, dbi: Int) {
        stackPush().use { stack ->
            val key = keyFunc(stack)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val dv = MDBVal.callocStack(stack).mv_size(maxBufferSize.toLong())

            val pp = stack.mallocPointer(1)

            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            mdb_put(txn, dbi, kv, dv, MDB_RESERVE)

            MemoryUtil.memCopy(data.position(0), dv.mv_data()!!)
            mdb_txn_commit(txn)

            committed = true
        }
    }

    fun readFromDB(env: Long, dbi: Int): M {
        TODO()
    }

    fun getBool(index: Int): Boolean {
        return data[SIZE_MARKER_SIZE + offsets[index]] != 0.toByte()
    }

    fun setBool(index: Int, value: Boolean) {
        data.put(SIZE_MARKER_SIZE + offsets[index], if (value) 1.toByte() else 0.toByte())
        committed = false
    }

    fun getByte(index: Int): Byte {
        return data[SIZE_MARKER_SIZE + offsets[index]]
    }

    fun setByte(index: Int, value: Byte) {
        data.put(SIZE_MARKER_SIZE + offsets[index], value)
        committed = false
    }

    fun getShort(index: Int): Short {
        return dataShorts[offsets[index]]
    }

    fun setShort(index: Int, value: Short) {
        dataShorts.put(offsets[index], value)
        committed = false
    }

    fun getChar(index: Int): Char {
        return dataChars[offsets[index]]
    }

    fun setChar(index: Int, value: Char) {
        dataChars.put(offsets[index], value)
        committed = false
    }

    fun getInt(index: Int): Int {
        return dataInts[offsets[index]]
    }

    fun setInt(index: Int, value: Int) {
        dataInts.put(offsets[index], value)
        committed = false
    }

    fun getFloat(index: Int): Float {
        return dataFloats[offsets[index]]
    }

    fun setFloat(index: Int, value: Float) {
        dataFloats.put(offsets[index], value)
        committed = false
    }

    fun getLong(index: Int): Long {
        return dataLongs[offsets[index]]
    }

    fun setLong(index: Int, value: Long) {
        dataLongs.put(offsets[index], value)
        committed = false
    }

    fun getDouble(index: Int): Double {
        return dataDoubles[offsets[index]]
    }

    fun setDouble(index: Int, value: Double) {
        dataDoubles.put(offsets[index], value)
        committed = false
    }

    fun setVarLong(index: Int, value: Long) {
        calculateVarSizeOffsets(index, value.getVarLongSize())
        data.writeVarLong(offsets[index], value)
        committed = false
    }

    fun getVarLong(index: Int): Long {
        return data.readVarLong(index)
    }

    fun getInd(name: String): Int {
        return -1
    }
}