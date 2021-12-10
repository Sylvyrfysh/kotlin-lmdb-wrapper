package com.nicholaspjohnson.kotlinlmdbwrapper

import java.util.*

object ByteSizes {
    fun fromKib(kib: Long): Long = kib * 1024
    fun fromMib(mib: Long): Long = mib * 1024 * 1024
    fun fromGib(gib: Long): Long = gib * 1024 * 1024 * 1024
    fun fromTib(tib: Long): Long = tib * 1024 * 1024 * 1024 * 1024
    fun fromPib(pib: Long): Long = pib * 1024 * 1024 * 1024 * 1024 * 1024
    fun fromEib(eib: Long): Long = eib * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
    fun fromKb(kb: Long): Long = kb * 1000
    fun fromMb(mb: Long): Long = mb * 1000 * 1000
    fun fromGb(gb: Long): Long = gb * 1000 * 1000 * 1000
    fun fromTb(tb: Long): Long = tb * 1000 * 1000 * 1000 * 1000
    fun fromPb(pb: Long): Long = pb * 1000 * 1000 * 1000 * 1000 * 1000
    fun fromEb(eb: Long): Long = eb * 1000 * 1000 * 1000 * 1000 * 1000 * 1000

    fun formatBytesAsString(size: Long, locale: Locale = Locale.getDefault()): String = when {
        size < 1024L -> "$size B"
        else -> {
            val z = (63 - size.countLeadingZeroBits()) / 10
            String.format(locale, "%.1f %ciB", size.toDouble() / (1L shl z * 10), "KMGTPE"[z - 1])
        }
    }
}