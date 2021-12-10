package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

import org.lwjgl.util.lmdb.LMDB

/**
 * An exception that signals that an object was attempted to be loaded from a database where the key did not exist.
 *
 * @constructor
 * Create a new [DataNotFoundException]
 *
 * @param[s] The message to give this exception
 */
class DataNotFoundException(s: String) : LMDBException(LMDB.MDB_NOTFOUND, s)
