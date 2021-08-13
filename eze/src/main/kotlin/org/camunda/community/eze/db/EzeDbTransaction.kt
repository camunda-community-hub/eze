package org.camunda.community.eze.db

import io.camunda.zeebe.db.TransactionOperation
import io.camunda.zeebe.db.ZeebeDbTransaction
import java.util.*
import kotlin.collections.HashSet

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

fun ByteArray.toBytes(length : Int) : Bytes {
    return Bytes(copyOfRange(0, length))
}

class EzeDbTransaction(val database : TreeMap<Bytes, Bytes>) : ZeebeDbTransaction  {

    private val transactionCache = TreeMap<Bytes, Bytes>()
    private val deletedKeys = HashSet<Bytes>()
    private var inCurrentTransaction = false

    fun resetTransaction() {
        rollback()
        inCurrentTransaction = true
    }

    fun isInCurrentTransaction(): Boolean {
        return inCurrentTransaction
    }

    override fun run(operations: TransactionOperation) {
        operations.run()
    }

    override fun commit() {
        inCurrentTransaction = false
        database.putAll(transactionCache)
        deletedKeys.forEach { database.remove(it) }
        transactionCache.clear()
    }

    override fun rollback() {
        inCurrentTransaction = false
        transactionCache.clear()
    }

    fun close() {
        transactionCache.clear()
    }

    fun put(keyBufferArray: ByteArray, keyLength: Int, valueBufferArray: ByteArray, valueLength: Int) {
        transactionCache[keyBufferArray.toBytes(keyLength)] = valueBufferArray.toBytes(valueLength)
    }

    fun get(keyBufferArray: ByteArray, keyLength: Int): ByteArray? {
        val keyBytes = keyBufferArray.toBytes(keyLength)
        val valueInCache = transactionCache[keyBytes]

        valueInCache?.let {
            return valueInCache.byteArray
        }

        return database[keyBytes]?.byteArray
    }

    fun delete(keyBufferArray: ByteArray, keyLength: Int) {
        val keyBytes = keyBufferArray.toBytes(keyLength)
        transactionCache.remove(keyBytes)
        deletedKeys.add(keyBytes)
    }

    fun newIterator(): EzeDbIterator {
//        val result = (transactionCache.asSequence() + database.asSequence()).distinct()
//            .groupBy({ it.key }, { it.value })
//            .mapValues { it.value.joinToString(",") }

        return EzeDbIterator(transactionCache)

//        TODO("Not yet implemented")
    }
}
