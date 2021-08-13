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
            val otherbyte = otherByteArray[i]

            when {
                ourByte < otherbyte -> {
                    return SMALLER
                }
                ourByte > otherbyte -> {
                    return BIGGER
                }
                else -> {
                    // = equals -> continue
                }
            }
        }

        if (byteArray.size == otherByteArray.size) {
            return EQUAL
        }
        else {
            // the other must be a longer array then
            return SMALLER
        }
    }
}

fun ByteArray.toBytes() : Bytes {
    return Bytes(this)
}

fun ByteArray.toBytes(length : Int) : Bytes {
    return Bytes(copyOfRange(0, length))
}

class EzeDbTransaction(val database : TreeMap<Bytes, Bytes>) : ZeebeDbTransaction  {

    private val transactionCache = TreeMap<Bytes, Bytes>()
    private val deletedKeys = HashSet<Bytes>()
    private var inCurrentTransaction = false

    // todo add get, delete and other stuff
//
//    @Throws(Exception::class)
//    fun put(
//        columnFamilyHandle: Long,
//        key: ByteArray?,
//        keyLength: Int,
//        value: ByteArray?,
//        valueLength: Int
//    ) {
//        RocksDbInternal.putWithHandle.invoke(
//            transaction, nativeHandle, key, keyLength, value, valueLength, columnFamilyHandle, false
//        )
//    }
//
//    @Throws(Exception::class)
//    operator fun get(
//        columnFamilyHandle: Long,
//        readOptionsHandle: Long,
//        key: ByteArray?,
//        keyLength: Int
//    ): ByteArray? {
//        return RocksDbInternal.getWithHandle.invoke(
//            transaction, nativeHandle, readOptionsHandle, key, keyLength, columnFamilyHandle
//        ) as ByteArray
//    }
//
//    @Throws(Exception::class)
//    fun delete(columnFamilyHandle: Long, key: ByteArray?, keyLength: Int) {
//        RocksDbInternal.removeWithHandle.invoke(
//            transaction, nativeHandle, key, keyLength, columnFamilyHandle, false
//        )
//    }
//
//    fun newIterator(options: ReadOptions?, handle: ColumnFamilyHandle?): RocksIterator? {
//        return transaction.getIterator(options, handle)
//    }

    fun resetTransaction() {
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
