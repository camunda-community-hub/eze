/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import io.camunda.zeebe.db.TransactionOperation
import io.camunda.zeebe.db.ZeebeDbTransaction
import java.util.*

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
        deletedKeys.clear()
        transactionCache.clear()
    }

    override fun rollback() {
        inCurrentTransaction = false
        transactionCache.clear()
        deletedKeys.clear()
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
        val map = TreeMap<Bytes, Bytes>()
        map.putAll(database)
        map.putAll(transactionCache)

        return EzeDbIterator(map)
    }
}
