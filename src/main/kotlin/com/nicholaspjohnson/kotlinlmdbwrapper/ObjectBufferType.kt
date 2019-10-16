package com.nicholaspjohnson.kotlinlmdbwrapper

import java.nio.ByteBuffer

/**
 * A limited set of options for initializing a [BaseLMDBObject].
 */
sealed class ObjectBufferType {
    /**
     * Tells us to use the user-defined buffer [buffer]
     *
     * @param[buffer] The user-defined buffer
     *
     * @constructor
     *
     * Sets the user-defined buffer to [buffer]
     */
    data class Buffer(val buffer: ByteBuffer) : ObjectBufferType()

    /**
     * Tells us to use the ddatabase-read buffer [buffer].
     * This signals to move the object before writing to it.
     *
     * @param[buffer] The database-defined buffer
     *
     * @constructor
     *
     * Sets the database-defined buffer to [buffer]
     */
    data class DBRead(val buffer: ByteBuffer) : ObjectBufferType()

    /**
     * Tells us to create a new, default buffer to place the object in.
     */
    object New : ObjectBufferType()
}