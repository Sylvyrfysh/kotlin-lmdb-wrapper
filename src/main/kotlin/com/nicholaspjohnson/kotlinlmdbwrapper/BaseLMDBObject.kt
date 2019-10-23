package com.nicholaspjohnson.kotlinlmdbwrapper

import org.lwjgl.system.MemoryStack.stackPush
import org.lwjgl.system.MemoryUtil
import org.lwjgl.util.lmdb.LMDB.*
import org.lwjgl.util.lmdb.MDBVal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.experimental.and
import kotlin.experimental.or

/**
 * A basic LMDBObject that is of the class [M] which extends [BaseLMDBObject].
 *
 * @constructor
 * Initialize the buffers and update the offsets
 *
 * @param[from] The way to create this object
 */
abstract class BaseLMDBObject<M : BaseLMDBObject<M>>(from: ObjectBufferType) {
    private lateinit var data: ByteBuffer

    private lateinit var types: Array<LMDBType<*>>
    private lateinit var nullables: Array<Boolean>
    private lateinit var varSizeTypes: Array<LMDBType<*>?>

    private lateinit var nameToInts: Map<String, Int>
    private lateinit var intsToName: Map<Int, String>

    private lateinit var offsets: IntArray
    private lateinit var constOffsets: IntArray
    private lateinit var sizes: IntArray
    private lateinit var constSizes: IntArray

    /**
     * Returns the size of this object in-DB
     */
    val size: Int
        get() = data.capacity()

    /**
     * The minimum buffer size [M] will ever need.
     */
    var minBufferSize: Int = -1
        private set
    /**
     * The maximum buffer size [M] will ever need.
     */
    var maxBufferSize: Int = -1
        private set
    private var constSizeSetSize: Int = -1
    private var requestedExtraSize: Int = 0
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
            initBuffers(ByteBuffer.allocate(minBufferSize).order(ByteOrder.nativeOrder()))
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

    /**
     * TODO: Any non-const size needs an on-disk size header. Required for branch merge, as varlongs may currently break disk load.
     *
     * Reads and sets the data and will redo offsets if read from DB
     */
    private fun initBuffers(newData: ByteBuffer) {
        checkBuffer(newData)
        data = newData
        data.position(0)
        if (justReadFromDB && hasVarSizeItems) {
            // Read from the DB- Recalculate our offsets
            var pushForward = 0
            val remainingTypes = varSizeTypes.copyOf()
            for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
                if (t!!.isConstSize) {
                    // We only have const-sized null types in this loop, so we currently don't need to do any special calculation
                    remainingTypes[index] = null
                }
            }

            for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
                // We only have potentially null, non-const-size types in this loop. Definitely need offset recalculation
                if (nullables[index]) {
                    offsets[index] += pushForward
                    val (isNull, isCompressed) = readNullableHeader(offsets[index])
                    val itemSize = if (isCompressed) {
                        1
                    } else {
                        if (t == LMDBType.LVarLong && isNull) {
                            1
                        } else {
                            t!!.getItemSizeFromDB(data, offsets[index] + 1)
                        }
                    }

                    if (itemSize > sizes[index]) {
                        pushForward += (itemSize - sizes[index])
                        sizes[index] = itemSize
                    }
                } else {
                    //easy route- just read it and update it
                    offsets[index] += pushForward
                    val itemSize = t!!.getItemSizeFromDB(data, offsets[index])
                    if (itemSize > sizes[index]) {
                        pushForward += (itemSize - sizes[index])
                        sizes[index] = itemSize
                    }
                }
                remainingTypes[index] = null
            }
            justReadFromDB = false
        }
    }

    private val haveTypes = HashMap<String, LMDBType<*>>()
    private val haveNullable = HashMap<String, Boolean>()
    private val minSizes = HashMap<String, Int>()

    /**
     * TODO: Move nullable into minSize here
     *
     * Adds an object to this object with the name [name] and [LMDBType] [type] that is [nullable].
     * If it is a variably sized attribute and annotated with [VarSizeDefault], this will put a custom minimum size on the object.
     */
    fun addType(name: String, type: LMDBType<*>, nullable: Boolean, varSizeDefault: VarSizeDefault? = null) {
        require(!haveTypes.containsKey(name)) { "Cannot have the same name twice!" }
        require(!isInit) { "Cannot add new DB items after first access!" }
        haveTypes[name] = type
        haveNullable[name] = nullable
        requestedExtraSize += if (nullable) 1 else 0
        if (varSizeDefault != null) {
            require(!type.isConstSize) { "Const-Sized types cannot have custom space allocated!" }
            require(varSizeDefault.minimumSize >= type.minSize) { "VarSizeDefault must be greater than or equal to the minimum size type!" }
            require(varSizeDefault.minimumSize <= type.maxSize) { "VarSizeDefault must be less than or equal to the maximum size type!" }
            if (type.clazz == String::class.java) { // need the size identifier before the varchar
                requestedExtraSize += ((varSizeDefault.minimumSize + (2 * varSizeDefault.minimumSize.toLong().getVarLongSize())) - type.minSize)
                minSizes[name] = varSizeDefault.minimumSize + (2 * varSizeDefault.minimumSize.toLong().getVarLongSize())
            } else {
                requestedExtraSize += (varSizeDefault.minimumSize - type.minSize)
                minSizes[name] = varSizeDefault.minimumSize
            }
        }
    }

    private fun setTypes() {
        require(haveTypes.isNotEmpty()) { "At least one type is required for an LMDBObject!" }
        val mapTypes = haveTypes.toSortedMap()
        val mapNullables = haveNullable.toSortedMap()
        this.types = mapTypes.values.toTypedArray()
        this.nullables = mapNullables.values.toTypedArray()
        val tNameMap = HashMap<String, Int>()
        mapTypes.keys.forEachIndexed { index, s ->
            tNameMap[s] = index
        }
        this.nameToInts = tNameMap
        this.intsToName = tNameMap.map { Pair(it.value, it.key) }.toMap()
        minBufferSize = types.map(LMDBType<*>::minSize).sum() + requestedExtraSize
        maxBufferSize = types.map(LMDBType<*>::maxSize).sum() + nullables.filter { it }.count()
        offsets = IntArray(types.size) { -1 }
        sizes = IntArray(types.size) { -1 }
        constOffsets = IntArray(types.size) { -1 }
        constSizes = IntArray(types.size) { -1 }
        calculateConstOffsets()
    }

    private fun calculateConstOffsets() {
        val remainingTypes = Array<LMDBType<*>?>(types.size) { types[it] }

        var byteOffset = 0 // for the size marker, we need an offset
        while (remainingTypes.asList().filterNotNull().count() > 0) {
            for ((index, t) in remainingTypes.withIndex()) {
                if (!nullables[index] && t!!.isConstSize) {
                    // We only have const-sized non-null types in this loop, so we can use minSize as size and get offsets that way
                    val size = t.minSize
                    offsets[index] = byteOffset
                    constOffsets[index] = byteOffset
                    sizes[index] = size
                    constSizes[index] = size
                    remainingTypes[index] = null
                    byteOffset += size
                    constSizeSetSize += size
                }
            }
            varSizeTypes = remainingTypes.copyOf()
            for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
                if (t!!.isConstSize) {
                    // We only have const-sized null types in this loop, so we can use minSize + 1 as size and get offsets that way
                    // This is not ok for reading from DB
                    val size = t.minSize + 1
                    offsets[index] = byteOffset
                    sizes[index] = size
                    remainingTypes[index] = null
                    byteOffset += size
                }
            }
            for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
                // We only have potentially null, non-const-size types in this loop, so we can use minSize + 1 as size and get offsets that way
                // This is not ok for reading from DB
                hasVarSizeItems = true
                val size = minSizes.getOrDefault(intsToName.getValue(index), t!!.minSize) + if(nullables[index]) 1 else 0
                offsets[index] = byteOffset
                sizes[index] = size
                remainingTypes[index] = null
                byteOffset += size
            }
        }
    }

    private fun calculateVarSizeOffsets(changedIdx: Int, newSize: Int) {
        if (sizes[changedIdx] >= newSize) {
            return
        }
        val remainingTypes = varSizeTypes.copyOf()

        val newOffsets = offsets.copyOf()
        val pushForward = (newSize - sizes[changedIdx])

        for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
            if (t!!.isConstSize && offsets[index] > offsets[changedIdx]) {
                // We only have const-sized null types in this loop, so we can use minSize + 1 as size and get offsets that way
                // This is not ok for reading from DB
                newOffsets[index] = offsets[index] + pushForward
                remainingTypes[index] = null
            }
        }

        for ((index, t) in remainingTypes.withIndex().filterNot { it.value == null }) {
            // We only have potentially null, non-const-size types in this loop, so we can use minSize + 1 as size and get offsets that way
            // This is not ok for reading from DB
            if (offsets[index] > offsets[changedIdx]) {
                newOffsets[index] = offsets[index] + pushForward
                remainingTypes[index] = null
            }
        }

        val writeBuf = ByteBuffer.allocate((newSize - sizes[changedIdx]) + sizes.sum())
            .order(ByteOrder.nativeOrder())
            .put(data)
        writeBuf.position(0)
        for (i in newOffsets.toList().withIndex().filterNot { it.value == offsets[it.index] }.sortedByDescending { it.value }) {
            val dCpy = ByteArray(sizes[i.index])
            data.position(offsets[i.index])
            data.get(dCpy)
            data.position(0)
            writeBuf.position(i.value)
            writeBuf.put(dCpy)
            writeBuf.position(0)
        }
        initBuffers(writeBuf)
        offsets = newOffsets
        sizes[changedIdx] = newSize
    }

    /**
     * Returns a [ByteArray] that contains a copy of the data in this object
     */
    fun getAllAsByteArray(): ByteArray {
        val retArr = ByteArray(data.capacity())
        data.position(0)
        data.get(retArr)
        return retArr
    }

    /**
     * Wrapper for assigning ibjects based on type and annotation
     */
    protected val db = LMDBBaseObjectProvider(this)

    /**
     * Returns a key that fits in the [keyBuffer]
     */
    protected abstract fun keyFunc(keyBuffer: ByteBuffer)

    /**
     * Writes only this object into the [dbi] of [env].
     * The key will be the return of [keyFunc].
     */
    fun writeInSingleTX(env: Long, dbi: Int) {
        stackPush().use { stack ->
            val key = stack.malloc(8)
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val dv = MDBVal.callocStack(stack).mv_size(data.capacity().toLong())

            val pp = stack.mallocPointer(1)

            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, 0, pp))
            val txn = pp.get(0)

            try {
                LMDB_CHECK(mdb_put(txn, dbi, kv, dv, MDB_RESERVE))

                data.position(0)
                dv.mv_data()!!.put(data)

                LMDB_CHECK(mdb_txn_commit(txn))
            } catch (t: Throwable) {
                mdb_txn_abort(txn)
                throw t
            }

            committed = true
        }
    }

    /**
     * Based on the key provided by [keyFunc], attempts to load all of the other data members.
     * If the key does not exist, a [DataNotFoundException] will be thrown.
     */
    @Throws(DataNotFoundException::class)
    fun readFromDB(env: Long, dbi: Int) {
        stackPush().use { stack ->
            val key = stack.malloc(8)
            keyFunc(key)
            key.position(0)
            val kv = MDBVal.callocStack(stack).mv_data(key)

            val pp = stack.mallocPointer(1)
            LMDB_CHECK(mdb_txn_begin(env, MemoryUtil.NULL, MDB_RDONLY, pp))
            val txn = pp.get(0)

            val dv = MDBVal.callocStack()
            val err = mdb_get(txn, dbi, kv, dv)
            if (err == MDB_NOTFOUND) {
                mdb_txn_abort(txn)
                throw DataNotFoundException("The key supplied does not have any data in the DB!")
            } else {
                try {
                    LMDB_CHECK(err)
                } catch (t: Throwable) {
                    mdb_txn_abort(txn)
                    throw t
                }
            }

            isOnDBAddress = true
            justReadFromDB = true
            initBuffers(dv.mv_data()!!)
            mdb_txn_abort(txn)
        }
    }

    internal fun getBool(index: Int): Boolean? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data[dataOffset] != 0.toByte()
    }

    internal fun setBool(index: Int, value: Boolean?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.put(dataOffset + 1, if (value) 1.toByte() else 0.toByte())
            }
        } else {
            data.put(dataOffset, if (value!!) 1.toByte() else 0.toByte())
        }
        committed = false
    }

    internal fun getByte(index: Int): Byte? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data[dataOffset]
    }

    internal fun setByte(index: Int, value: Byte?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.put(dataOffset + 1, value)
            }
        } else {
            data.put(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getShort(index: Int): Short? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getShort(dataOffset)
    }

    internal fun setShort(index: Int, value: Short?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putShort(dataOffset + 1, value)
            }
        } else {
            data.putShort(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getChar(index: Int): Char? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getChar(dataOffset)
    }

    internal fun setChar(index: Int, value: Char?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putChar(dataOffset + 1, value)
            }
        } else {
            data.putChar(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getInt(index: Int): Int? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getInt(dataOffset)
    }

    internal fun setInt(index: Int, value: Int?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putInt(dataOffset + 1, value)
            }
        } else {
            data.putInt(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getFloat(index: Int): Float? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getFloat(dataOffset)
    }

    internal fun setFloat(index: Int, value: Float?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putFloat(dataOffset + 1, value)
            }
        } else {
            data.putFloat(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getLong(index: Int): Long? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getLong(dataOffset)
    }

    internal fun setLong(index: Int, value: Long?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putLong(dataOffset + 1, value)
            }
        } else {
            data.putLong(dataOffset, value!!)
        }
        committed = false
    }

    internal fun getDouble(index: Int): Double? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.getDouble(dataOffset)
    }

    internal fun setDouble(index: Int, value: Double?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                data.putDouble(dataOffset + 1, value)
            }
        } else {
            data.putDouble(dataOffset, value!!)
        }
        committed = false
    }

    internal fun setVarLong(index: Int, value: Long?) {
        if (isOnDBAddress) {
            moveFromDBAddress()
        }
        // Disk size byte
        val newSize = 1 + if (nullables[index]) {
            (value?.getVarLongSize() ?: 0) + 1 //size on disk plus nullable header
        } else {
            value!!.getVarLongSize()
        }
        calculateVarSizeOffsets(index, newSize)
        val dataOffset = offsets[index]
        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            if (value != null) {
                val diskSize = sizes[index].toLong()
                data.writeVarLong(dataOffset + 1, diskSize)
                data.writeVarLong(dataOffset + 2, value)
            }
        } else {
            val diskSize = sizes[index].toLong()
            data.writeVarLong(dataOffset, diskSize)
            data.writeVarLong(dataOffset + 1, value!!)
        }
        committed = false
    }

    internal fun getVarLong(index: Int): Long? {
        var dataOffset = offsets[index]
        if (nullables[index]) {
            val (isNull, _) = readNullableHeader(dataOffset)
            if (isNull) {
                return null
            }
            dataOffset += 1
        }
        return data.readVarLong(dataOffset + 1)
    }

    internal fun setVarChar(index: Int, value: String?) {
        var dataOffset = offsets[index]

        fun writeStringToBuffer(value: String) {
            val utf8Data = MemoryUtil.memUTF8(value, false)
            val memUTF8Len = utf8Data.capacity()
            val stringDiskSize = memUTF8Len + (2 * memUTF8Len.toLong().getVarLongSize())
            require(stringDiskSize <= types[index].maxSize) { "UTF-8 encoded value exceeds VarChar size!" }
            if (isOnDBAddress) {
                moveFromDBAddress()
            }
            calculateVarSizeOffsets(index, stringDiskSize + if(nullables[index]) 1 else 0)
            val diskSizeLen = sizes[index].toLong().getVarLongSize()
            val diskSize = sizes[index].toLong()
            data.writeVarLong(dataOffset, diskSize)
            val currentSizeLen = memUTF8Len.toLong().getVarLongSize()
            data.writeVarLong(dataOffset + diskSizeLen, memUTF8Len.toLong())
            data.position(dataOffset + diskSizeLen + currentSizeLen)
            utf8Data.position(0)
            data.put(utf8Data)
            data.position(0)
        }

        if (nullables[index]) {
            writeNullableHeader(dataOffset, value == null, false)
            dataOffset += 1
            if (value != null) {
                writeStringToBuffer(value)
            } else {
                val diskSize = sizes[index].toLong()
                data.writeVarLong(dataOffset, diskSize)
            }
        } else {
            writeStringToBuffer(value!!)
        }

        committed = false
    }

    internal fun getVarChar(index: Int): String? {
        var baseOffset = offsets[index]

        fun readNonNullString(): String {
            val diskSizeLen = data.readVarLong(baseOffset).getVarLongSize()
            if (diskSizeLen == 0) {
                return ""
            }
            val currentSize = data.readVarLong(baseOffset + diskSizeLen)
            return if (data.isDirect) {
                MemoryUtil.memUTF8(
                    data,
                    currentSize.toInt(),
                    baseOffset + diskSizeLen + currentSize.getVarLongSize()
                )
            } else {
                stackPush().use { stack ->
                    val dirBuf = stack.malloc(currentSize.toInt())
                    val oldLimit = data.limit()
                    data.limit(baseOffset + diskSizeLen + currentSize.getVarLongSize() + currentSize.toInt())
                        .position(baseOffset + diskSizeLen + currentSize.getVarLongSize())
                    dirBuf.put(data)
                    data.position(0).limit(oldLimit)

                    dirBuf.position(0)
                    MemoryUtil.memUTF8(dirBuf, currentSize.toInt())
                }
            }
        }

        if (nullables[index]) {
            val header = readNullableHeader(baseOffset)
            baseOffset += 1
            if (header.first) {
                return null
            }
            return readNonNullString()
        } else {
            return readNonNullString()
        }
    }

    /**
     * Writes a single byte that marks whether this object is null and/or compacted.
     *
     * If [isNull] is true, [isCompacted] will be written, otherwise it will be a 0 byte.
     */
    private fun writeNullableHeader(offset: Int, isNull: Boolean, isCompacted: Boolean) {
        var nullableInfoByte: Byte = 0
        if (isNull) {
            nullableInfoByte = nullableInfoByte or NULLABLE_NULL_BIT
            if (isCompacted) {
                nullableInfoByte = nullableInfoByte or NULLABLE_COMPACTED_BIT
            }
        }
        data.put(offset, nullableInfoByte)
    }

    /**
     * Reads a single byte that tells whether this object is null and/or compacted.
     * Returns a [Pair] of booleans that are (null, compacted). If null is false, compacted will be false.
     * If compacted is true, there is no more data for this object after this byte.
     */
    private fun readNullableHeader(offset: Int): Pair<Boolean, Boolean> {
        val nullableInfoByte: Byte = data[offset]
        return Pair(
            (nullableInfoByte and NULLABLE_NULL_BIT) == NULLABLE_NULL_BIT,
            (nullableInfoByte and NULLABLE_COMPACTED_BIT) == NULLABLE_COMPACTED_BIT
        )
    }

    private fun moveFromDBAddress() {
        require(isOnDBAddress) { "Cannot move off of DB Address if we're not on it!" }
        val x = ByteBuffer.allocate(data.capacity())
            .order(ByteOrder.nativeOrder())
            .put(data)
        x.position(0)
        initBuffers(x)
        isOnDBAddress = false
    }

    /**
     * Helper function to turn [name] into the underlying index for a property.
     * Returns the index [name] is at.
     */
    fun getInd(name: String): Int {
        if (!isInit) {
            setUsed()
        }
        return nameToInts[name] ?: error("Name $name is not present")
    }

    companion object {
        private const val NULLABLE_NULL_BIT = (1 shl 0).toByte()
        private const val NULLABLE_COMPACTED_BIT = (1 shl 1).toByte()
    }
}