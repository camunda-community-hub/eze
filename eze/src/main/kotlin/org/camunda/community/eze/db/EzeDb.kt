/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import io.camunda.zeebe.db.*
import io.camunda.zeebe.db.impl.DbNil
import java.io.File
import java.util.*

class EzeDb<ColumnFamilyType : Enum<ColumnFamilyType>> : ZeebeDb<ColumnFamilyType> {

    private val database = TreeMap<Bytes, Bytes>()

    override fun close() {
        database.clear()
    }

    override fun <KeyType : DbKey, ValueType : DbValue> createColumnFamily(
        columnFamily: ColumnFamilyType,
        context: TransactionContext,
        keyInstance: KeyType,
        valueInstance: ValueType
    ): ColumnFamily<KeyType, ValueType> {
        return EzeDbColumnFamily(database, columnFamily, context, keyInstance, valueInstance)
    }

    override fun createContext(): TransactionContext {
        return EzeDbTransactionContext(database)
    }

    override fun createSnapshot(snapshotDir: File?) {
        TODO("No snapshots supported")
    }

    override fun getProperty(propertyName: String?): Optional<String> {
        TODO("Not yet implemented")
    }

    override fun isEmpty(column: ColumnFamilyType, context: TransactionContext): Boolean {
        return createColumnFamily(column, context, DbNullKey.INSTANCE, DbNil.INSTANCE).isEmpty
    }
}
