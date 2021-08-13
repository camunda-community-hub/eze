package org.camunda.community.eze.db

class Bytes(val byteArray: ByteArray) : Comparable<Bytes> {
    override fun equals(other: Any?): Boolean =
        this === other || other is Bytes && this.byteArray contentEquals other.byteArray
    override fun hashCode(): Int = byteArray.contentHashCode()
    override fun toString(): String = byteArray.contentToString()

    override fun compareTo(other: Bytes): Int {
        val EQUAL = 0
        val SMALLER = -1
        val BIGGER = 1

        val otherByteArray = other.byteArray

        for (i in byteArray.indices) {
            if (i >= otherByteArray.size)
            {
                return BIGGER
            }

            val ourByte = byteArray[i]
            val otherByte = otherByteArray[i]

            when {
                ourByte < otherByte -> {
                    return SMALLER
                }
                ourByte > otherByte -> {
                    return BIGGER
                }
                else -> {
                    // = equals -> continue
                }
            }
        }

        return if (byteArray.size == otherByteArray.size) {
            EQUAL
        } else {
            // the other must be a longer array then
            SMALLER
        }
    }
}
