package com.nicholaspjohnson.kotlinlmdbwrapper.lmdb

open class LMDBException(val rc: Int, mdbStrerror: String) : Exception(mdbStrerror)
