package com.nicholaspjohnson.kotlinlmdbwrapper

/**
 * Used on a object with type Long to specify to store it as a VarLong
 */
annotation class VarLong

/**
 * Used on a String to specify that it is a VarChar and give the [maxLength]
 *
 * @property maxLength the max length of the VarChar
 */
@Deprecated("This is no longer applicable and does nothing.")
annotation class VarChar(val maxLength: Int)

/**
 * Used on any var-sized object to specify the [minimumSize] that should be reserved for it
 *
 * @property minimumSize the minimum size of this object in-DB
 */
@Deprecated("This is no longer applicable and does nothing.")
annotation class VarSizeDefault(val minimumSize: Int)