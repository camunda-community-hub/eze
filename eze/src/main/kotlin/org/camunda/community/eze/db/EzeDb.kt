package org.camunda.community.eze.db

import io.camunda.zeebe.db.*
import io.camunda.zeebe.db.impl.DbNil
import io.camunda.zeebe.engine.state.ZbColumnFamilies
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
