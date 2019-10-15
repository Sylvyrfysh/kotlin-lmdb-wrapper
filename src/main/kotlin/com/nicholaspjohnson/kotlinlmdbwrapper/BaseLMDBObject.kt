package com.nicholaspjohnson.kotlinlmdbwrapper

import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.ByteTypedRWP
import com.nicholaspjohnson.kotlinlmdbwrapper.rwps.ShortTypedRWP
import org.lwjgl.system.MemoryStack
import org.lwjgl.system.MemoryStack.stackPush
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.io.Serializable
import java.nio.*
import kotlin.properties.ReadWriteProperty

abstract class BaseLMDBObject<M : BaseLMDBObject<M>>(from: ObjectBufferType) {
    private lateinit var data: ByteBuffer
    private lateinit var dataShorts: ShortBuffer
    private lateinit var dataChars: CharBuffer
    private lateinit var dataInts: IntBuffer
    private lateinit var dataLongs: LongBuffer
    private lateinit var dataFloats: FloatBuffer
    private lateinit var dataDoubles: DoubleBuffer

    private lateinit var types: Array<LMDBType<*>>
    private lateinit var varSizeTypes: Array<LMDBType<*>?>

    private lateinit var nameToInts: Map<String, Int>
    private lateinit var intsToName: Map<Int, String>

    private lateinit var offsets: IntArray
    private lateinit var constOffsets: IntArray
    private lateinit var sizes: IntArray
    private lateinit var constSizes: IntArray

    var minBufferSize: Int = -1
        private set
    var maxBufferSize: Int = -1
        private set
    private var constSizeSetSize: Int = -1
    private var requestedExtraSize: Int = 0
    private var maxAlign: Int = -1
    private var justReadFromDB = false

    /**
     * True if this object has been committed to the DB, or read from the DB and not modified.
     */
    var committed: Boolean
        private set
    private var isOnDBAddress: Boolean
    private var firstBuf: ByteBuffer?
    private var isInit = false
    private var hasVarSizeItems = false

    private val preferredOrder = LMDBType.run { listOf(LLong, LDouble, LInt, LFloat, LShort, LChar, LByte, LBool) }

    init {
        when (from) {
            is ObjectBufferType.New -> {
                firstBuf = null
                isOnDBAddress = false
                committed = false
            }
            is ObjectBufferType.Buffer -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
                isOnDBAddress = false
                committed = false
            }
            is ObjectBufferType.DBRead -> {
                checkBuffer(from.buffer)
                firstBuf = from.buffer
                isOnDBAddress = true
                committed = true
            }
        }
    }

    private fun setUsed() {
        setTypes()
        if (firstBuf == null) { // create new
            initBuffers(ByteBuffer.allocate(minBufferSize + requestedExtraSize).order(ByteOrder.nativeOrder()))
        } else {
            initBuffers(firstBuf!!)
        }
        firstBuf = null
        isInit = true
    }

    private fun checkBuffer(buffer: ByteBuffer) {
        require(buffer.capacity() >= minBufferSize) { "Provided buffers must have at least minBufferSize capacity!" }
        require(buffer.order() == ByteOrder.nativeOrder()) { "The buffer order must be equal to ByteOrder.nativeOrder()!" }
    }

    private fun initBuffers(newData: ByteBuffer) {
        checkBuffer(newData)
        data = newData
        data.position(0)
        dataShorts = data.asShortBuffer()
        dataChars = data.asCharBuffer()
        dataInts = data.asIntBuffer()
        dataLongs = data.asLongBuffer()
        dataFloats = data.asFloatBuffer()
        dataDoubles = data.asDoubleBuffer()
        if (justReadFromDB && hasVarSizeItems) {
            // Read from the DB- Recalculate our offsets
            var pushForward = 0
            for ((index, t) in varSizeTypes.withIndex()) {
                if (t == null) {
                    continue
                }

                val normalSize = minSizes.getOrDefault(intsToName.getValue(index), t.minSize)
                val currentSize = normalSize.coerceAtLeast(t.getItemSizeFromDB(data, offsets[index] + pushForward))
                if (pushForward > 0) {
                    offsets[index] += pushForward
                }
                sizes[index] = currentSize
                if (currentSize > normalSize) {
                    pushForward += (currentSize - normalSize)
                }
            }
            justReadFromDB = false
        }
    }

    private val haveTypes = HashMap<String, LMDBType<*>>()
    private val minSizes = HashMap<String, Int>()
    fun addType(name: String, type: LMDBType<*>, varSizeDefault: VarSizeDefault? = null) {
        require(!haveTypes.containsKey(name)) { "Cannot have the same name twice!" }
        require(!isInit) { "Cannot add new DB items after first access!" }
        haveTypes[name] = type
        if(varSizeDefault != null) {
            require(!type.isConstSize) { "Const-Sized types cannot have custom space allocated!" }
            require(varSizeDefault.min >= type.minSize) { "VarSizeDefault must be greater than or equal to the minimum size type!" }
            require(varSizeDefault.min <= type.maxSize) { "VarSizeDefault must be less than or equal to the maximum size type!" }
            if (type.clazz == String::class.java) { // need the size identifier before the varchar
                requestedExtraSize += ((varSizeDefault.min + (2 * varSizeDefault.min.toLong().getVarLongSize())) - type.minSize)
                minSizes[name] = varSizeDefault.min + (2 * varSizeDefault.min.toLong().getVarLongSize())
            } else {
                requestedExtraSize += (varSizeDefault.min - type.minSize)
                minSizes[name] = varSizeDefault.min
            }
        }
    }

    private fun setTypes() {
        require(haveTypes.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        val mapTypes = haveTypes.toSortedMap()
        this.types = mapTypes.values.toTypedArray()
        val tNameMap = HashMap<String, Int>()
        mapTypes.keys.forEachIndexed { index, s ->
            tNameMap[s] = index
        }
        this.nameToInts = tNameMap
        this.intsToName = tNameMap.map { Pair(it.value, it.key) }.toMap()
        minBufferSize = types.map(LMDBType<*>::minSize).sum()
        maxBufferSize = types.map(LMDBType<*>::maxSize).sum()
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
        while (remainingTypes.asList().filterNotNull().count() > 0) {
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

                varSizeTypes = remainingTypes.copyOf()
                //non-const sized things will fall here
                for ((index, t) in remainingTypes.withIndex()) {
                    if (t == null) {
                        continue
                    }

                    hasVarSizeItems = true
                    val size = minSizes.getOrDefault(intsToName.getValue(index), t.minSize)
                    offsets[index] = byteOffset
                    sizes[index] = size
                    remainingTypes[index] = null
                    byteOffset += size
                }
            }
        }
    }

    private fun calculateVarSizeOffsets(changedIdx: Int, newSize: Int) {
        if (sizes[changedIdx] >= newSize) {
            return
        }
        val remainingTypes = varSizeTypes.copyOf()

        var needsChange = false
        val newOffsets = offsets.copyOf()

        while (remainingTypes.asList().filterNotNull().count() > 0) {
            for ((index, t) in remainingTypes.withIndex()) {
                if (t == null) {
                    continue
                }

                if (needsChange) {
                    newOffsets[index] += (newSize - sizes[changedIdx])
                } else if (index == changedIdx) {
                    needsChange = true
                }
                remainingTypes[index] = null
            }
        }

        if (!newOffsets.contentEquals(offsets)) {
            val newBuffer = data.capacity() < ((newSize - sizes[changedIdx]) + sizes.sum())
            val writeBuf =
                if (newBuffer) {
                    ByteBuffer.allocate((newSize - sizes[changedIdx]) + sizes.sum())
                        .order(ByteOrder.nativeOrder())
                        .put(data)
                        .position(0)
                } else {
                    data
                }
            for (i in newOffsets.toList().withIndex().filterNot { it.value == offsets[it.index] }.sortedByDescending { it.value }) {
                val dCpy = ByteArray(sizes[i.index])
                data.position(offsets[i.index]).get(dCpy)
                writeBuf.position(i.value).put(dCpy)
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

    protected val db = LMDBBaseObjectProvider(this)

    protected abstract fun keyFunc(stack: MemoryStack): ByteBuffer

    fun writeInSingleTX(env: Long, dbi: Int) {
        stackPush().use { stack ->
            val key = keyFunc(stack)
            val kv = MDBVal.callocStack(stack).mv_data(key.position(0))

            val dv = MDBVal.callocStack(stack).mv_size(data.capacity().toLong())

            val pp = stack.mallocPointer(1)

            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            mdb_put(txn, dbi, kv, dv, MDB_RESERVE)

            dv.mv_data()!!.put(data.position(0))
            mdb_txn_commit(txn)

            committed = true
        }
    }

    @Throws(DataNotFoundException::class)
    fun readFromDB(env: Long, dbi: Int) {
        stackPush().use { stack ->
            val key = keyFunc(stack)
            val kv = MDBVal.callocStack(stack).mv_data(key.position(0))

            val pp = stack.mallocPointer(1)
            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
            val txn = pp.get(0)

            val dv = MDBVal.callocStack()
            val err = mdb_get(txn, dbi, kv, dv)
            if (err == MDB_NOTFOUND) {
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                LMDB_CHECK(err)
            }

            isOnDBAddress = true
            justReadFromDB = true
            initBuffers(dv.mv_data()!!)
            mdb_txn_commit(txn)
        }
    }

    fun getBool(index: Int): Boolean {
        return data[offsets[index]] != 0.toByte()
    }

    fun setBool(index: Int, value: Boolean) {
        if (isOnDBAddress) {
            TODO()
        }
        data.put(offsets[index], if (value) 1.toByte() else 0.toByte())
        committed = false
    }

    fun getByte(index: Int): Byte {
        return data[offsets[index]]
    }

    fun setByte(index: Int, value: Byte) {
        if (isOnDBAddress) {
            TODO()
        }
        data.put(offsets[index], value)
        committed = false
    }

    fun getShort(index: Int): Short {
        return dataShorts[offsets[index]]
    }

    fun setShort(index: Int, value: Short) {
        if (isOnDBAddress) {
            TODO()
        }
        dataShorts.put(offsets[index], value)
        committed = false
    }

    fun getChar(index: Int): Char {
        return dataChars[offsets[index]]
    }

    fun setChar(index: Int, value: Char) {
        if (isOnDBAddress) {
            TODO()
        }
        dataChars.put(offsets[index], value)
        committed = false
    }

    fun getInt(index: Int): Int {
        return dataInts[offsets[index]]
    }

    fun setInt(index: Int, value: Int) {
        if (isOnDBAddress) {
            TODO()
        }
        dataInts.put(offsets[index], value)
        committed = false
    }

    fun getFloat(index: Int): Float {
        return dataFloats[offsets[index]]
    }

    fun setFloat(index: Int, value: Float) {
        if (isOnDBAddress) {
            TODO()
        }
        dataFloats.put(offsets[index], value)
        committed = false
    }

    fun getLong(index: Int): Long {
        return dataLongs[offsets[index]]
    }

    fun setLong(index: Int, value: Long) {
        if (isOnDBAddress) {
            TODO()
        }
        dataLongs.put(offsets[index], value)
        committed = false
    }

    fun getDouble(index: Int): Double {
        return dataDoubles[offsets[index]]
    }

    fun setDouble(index: Int, value: Double) {
        if (isOnDBAddress) {
            TODO()
        }
        dataDoubles.put(offsets[index], value)
        committed = false
    }

    fun setVarLong(index: Int, value: Long) {
        if (isOnDBAddress) {
            TODO()
        }
        calculateVarSizeOffsets(index, value.getVarLongSize())
        data.writeVarLong(offsets[index], value)
        committed = false
    }

    fun getVarLong(index: Int): Long {
        return data.readVarLong(offsets[index])
    }

    fun setVarChar(index: Int, value: String) {
        val utf8Data = MemoryUtil.memUTF8(value, true)
        val memUTF8Len = utf8Data.capacity()
        val fullSize = memUTF8Len + (2 * memUTF8Len.toLong().getVarLongSize())
        require(fullSize <= types[index].maxSize) { "Item exceeds VarChar size!" }
        if (isOnDBAddress) {
            TODO()
        }
        calculateVarSizeOffsets(index, fullSize)
        val diskSizeLen = sizes[index].toLong().getVarLongSize()
        val diskSize = sizes[index].toLong()
        data.writeVarLong(offsets[index], diskSize)
        val currentSizeLen = memUTF8Len.toLong().getVarLongSize()
        data.writeVarLong(offsets[index] + diskSizeLen, memUTF8Len.toLong())
        data.position(offsets[index] + diskSizeLen + currentSizeLen).put(utf8Data.position(0)).position(0)

        committed = false
    }

    fun getVarChar(index: Int): String {
        val diskSizeLen = data.readVarLong(offsets[index]).getVarLongSize()
        if (diskSizeLen == 0) {
            return ""
        }
        val currentSize = data.readVarLong(offsets[index] + diskSizeLen)
        return if (data.isDirect) {
            MemoryUtil.memUTF8(data, currentSize.toInt(), offsets[index] + diskSizeLen + currentSize.getVarLongSize())
        } else {
            stackPush().use { stack ->
                val dirBuf = stack.malloc(currentSize.toInt())
                val oldLimit = data.limit()
                dirBuf.put(data.limit(offsets[index] + diskSizeLen + currentSize.getVarLongSize() + currentSize.toInt()).position(offsets[index] + diskSizeLen + currentSize.getVarLongSize()))
                data.position(0).limit(oldLimit)

                dirBuf.position(0)
                MemoryUtil.memUTF8(dirBuf, currentSize.toInt())
            }
        }
    }

    fun getInd(name: String): Int {
        if (!isInit) {
            setUsed()
        }
        return nameToInts[name] ?: error("Name $name is not present")
    }
}