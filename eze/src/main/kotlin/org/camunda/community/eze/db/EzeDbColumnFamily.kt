/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import io.camunda.zeebe.db.*
import io.camunda.zeebe.protocol.ColumnFamilyScope
import io.camunda.zeebe.protocol.EnumValue
import io.camunda.zeebe.protocol.ScopedColumnFamily
import io.camunda.zeebe.util.buffer.BufferUtil.startsWith
import org.agrona.DirectBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer
import java.util.function.Consumer


class EzeDbColumnFamily<ColumnFamilyNames,
        KeyType : DbKey,
        ValueType : DbValue>(
    val database: TreeMap<Bytes, Bytes>,
    val columnFamily: ColumnFamilyNames,
    val context: TransactionContext,
    val keyInstance: KeyType,
    val valueInstance: ValueType
) : ColumnFamily<KeyType, ValueType>
        where ColumnFamilyNames : Enum<ColumnFamilyNames>,
              ColumnFamilyNames : EnumValue {

    private val columnFamilyContext = ColumnFamilyContext(columnFamily.value.toLong())

    private fun ensureInOpenTransaction(
        context: TransactionContext,
        operation: TransactionConsumer
    ) {
        context.runInTransaction {
            operation.run(context.currentTransaction as EzeDbTransaction)
        }
    }

    override fun insert(key: KeyType, value: ValueType) {
        if (exists(key)) {
            throw IllegalStateException("An entry with the key $key already exists.")
        }
        put(key, value)
    }

    override fun update(key: KeyType, value: ValueType) {
        if (!exists(key)) {
            throw IllegalStateException("An entry with the key $key doesn't exist.")
        }
        put(key, value)
    }

    override fun upsert(key: KeyType, value: ValueType) {
        put(key, value)
    }

    private fun put(key: KeyType, value: ValueType) {
        ensureInOpenTransaction(
            context
        ) { transaction ->
            columnFamilyContext.writeKey(key)
            val valueLength = columnFamilyContext.writeValue(value)

            transaction.put(
                columnFamilyContext.keyBufferArray,
                columnFamilyContext.keyLength,
                columnFamilyContext.valueBufferArray,
                valueLength
            )
        }
    }


    override fun get(key: KeyType): ValueType? {
        columnFamilyContext.writeKey(key)
        val valueBuffer: DirectBuffer? = getValue(context, columnFamilyContext)
        if (valueBuffer != null) {
            valueInstance.wrap(valueBuffer, 0, valueBuffer.capacity())
            return valueInstance
        }
        return null
    }

    override fun get(key: KeyType, fallback: java.util.function.Supplier<ValueType>): ValueType? {
        columnFamilyContext.writeKey(key)
        val valueBuffer: DirectBuffer? = getValue(context, columnFamilyContext)
        if (valueBuffer != null) {
            val value = fallback.get()
            value.wrap(valueBuffer, 0, valueBuffer.capacity())
            return value
        }
        return null
    }

    private fun getValue(
        context: TransactionContext, columnFamilyContext: ColumnFamilyContext
    ): DirectBuffer? {
        ensureInOpenTransaction(
            context
        ) { transaction ->
            val value: ByteArray? = transaction.get(
                columnFamilyContext.keyBufferArray,
                columnFamilyContext.keyLength
            )

            columnFamilyContext.wrapValueView(value)
        }
        return columnFamilyContext.valueView
    }

    override fun forEach(consumer: Consumer<ValueType>) {
        forEach(context, consumer)
    }

    override fun forEach(consumer: BiConsumer<KeyType, ValueType>) {
        forEach(context, consumer)
    }

    override fun whileTrue(visitor: KeyValuePairVisitor<KeyType, ValueType>) {
        whileTrue(context, visitor)
    }

    override fun whileEqualPrefix(
        keyPrefix: DbKey, visitor: BiConsumer<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyPrefix, visitor)
    }

    override fun whileEqualPrefix(
        keyPrefix: DbKey, visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyPrefix, visitor)
    }

    override fun whileEqualPrefix(
        keyPrefix: DbKey,
        startAtKey: KeyType?,
        visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileWithPrefix(
            context = context,
            prefix = keyPrefix,
            keyInstance = keyInstance,
            valueInstance = valueInstance,
            visitor = visitor,
            startAtKey = startAtKey
        )
    }

    override fun deleteExisting(key: KeyType) {
        if (!exists(key)) {
            throw IllegalStateException("An entry with the key $key doesn't exist.")
        }
        delete(key)
    }

    override fun deleteIfExists(key: KeyType) {
        delete(key)
    }

    private fun delete(key: KeyType) {
        columnFamilyContext.writeKey(key)
        ensureInOpenTransaction(
            context
        ) { transaction ->
            transaction.delete(
                columnFamilyContext.keyBufferArray,
                columnFamilyContext.keyLength
            )
        }
    }

    override fun exists(key: KeyType): Boolean {
        columnFamilyContext.wrapValueView(ByteArray(0))
        ensureInOpenTransaction(
            context
        ) { transaction ->
            columnFamilyContext.writeKey(key)
            getValue(context, columnFamilyContext)
        }
        return !columnFamilyContext.isValueViewEmpty
    }

    override fun isEmpty(): Boolean {
        val isEmpty = AtomicBoolean(true)
        whileEqualPrefix(
            context,
            keyInstance,
            valueInstance,
            KeyValuePairVisitor<DbKey, DbValue> { key: DbKey?, value: DbValue? ->
                isEmpty.set(false)
                false
            })
        return isEmpty.get()
    }

    override fun count(): Long {
        var count = 0L
        whileTrue(KeyValuePairVisitor { _, _ ->
            count++
            true
        })
        return count
    }

    override fun countEqualPrefix(keyPrefix: DbKey): Long {
        var count = 0L
        whileEqualPrefix(keyPrefix, KeyValuePairVisitor { _, _ ->
            count++
            true
        })
        return count
    }

    override fun forEachKey(consumer: Consumer<KeyType>) {
        whileTrue(KeyVisitor { key ->
            consumer.accept(key)
            true
        })
    }

    override fun whileTrue(visitor: KeyVisitor<KeyType>) {
        whileEqualPrefix(DbNullKey(), visitor)
    }

    override fun whileTrue(startAtKey: KeyType?, visitor: KeyVisitor<KeyType>) {
        startAtKey?.let {
            whileWithPrefix(
                context = context,
                prefix = DbNullKey(),
                keyInstance = keyInstance,
                visitor = visitor,
                startAtKey = it
            )
        } ?: whileTrue(visitor)
    }

    override fun whileEqualPrefix(keyPrefix: DbKey, visitor: Consumer<KeyType>) {
        whileEqualPrefix(keyPrefix, KeyVisitor { key ->
            visitor.accept(key)
            true
        })
    }

    override fun whileEqualPrefix(keyPrefix: DbKey, visitor: KeyVisitor<KeyType>) {
        whileWithPrefix(
            context = context,
            prefix = keyPrefix,
            keyInstance = keyInstance,
            visitor = visitor
        )
    }

    override fun whileEqualPrefix(
        keyPrefix: DbKey,
        startAtKey: KeyType?,
        visitor: KeyVisitor<KeyType>
    ) {
        whileWithPrefix(
            context = context,
            prefix = keyPrefix,
            keyInstance = keyInstance,
            visitor = visitor,
            startAtKey = startAtKey
        )
    }

    override fun whileTrueReverse(
        startAtKey: KeyType,
        visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileTrue(startAtKey, visitor)
    }

    override fun whileTrueReverse(startAtKey: KeyType, visitor: KeyVisitor<KeyType>) {
        whileTrue(startAtKey, visitor)
    }

    override fun partitionScope(): ColumnFamilyScope {
        return (columnFamily as? ScopedColumnFamily)?.partitionScope()
            ?: ColumnFamilyScope.PARTITION_LOCAL
    }

    fun forEach(context: TransactionContext, consumer: Consumer<ValueType>) {
        whileEqualPrefix(
            context,
            keyInstance,
            valueInstance,
            BiConsumer { ignore: KeyType, value: ValueType ->
                consumer.accept(
                    value
                )
            })
    }

    fun forEach(
        context: TransactionContext, consumer: BiConsumer<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyInstance, valueInstance, consumer)
    }

    private fun whileTrue(
        context: TransactionContext, visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyInstance, valueInstance, visitor)
    }

    private fun whileEqualPrefix(
        context: TransactionContext,
        keyPrefix: DbKey,
        visitor: BiConsumer<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyPrefix, keyInstance, valueInstance, visitor)
    }

    private fun whileEqualPrefix(
        context: TransactionContext,
        keyPrefix: DbKey,
        visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, keyPrefix, keyInstance, valueInstance, visitor)
    }

    private fun newIterator(context: TransactionContext): EzeDbIterator {
        val currentTransaction = context.currentTransaction as EzeDbTransaction
        return currentTransaction.newIterator()
    }

    private fun <KeyType : DbKey?, ValueType : DbValue?> whileEqualPrefix(
        context: TransactionContext,
        prefix: DbKey?,
        keyInstance: KeyType,
        valueInstance: ValueType,
        visitor: BiConsumer<KeyType, ValueType>
    ) {
        whileEqualPrefix(
            context,
            prefix,
            keyInstance,
            valueInstance,
            KeyValuePairVisitor { k: KeyType, v: ValueType ->
                visitor.accept(k, v)
                true
            })
    }

    /**
     * This method is used mainly from other iterator methods to iterate over column family entries,
     * which are prefixed with column family key.
     */
    private fun <KeyType : DbKey?, ValueType : DbValue?> whileEqualPrefix(
        context: TransactionContext,
        keyInstance: KeyType,
        valueInstance: ValueType,
        visitor: BiConsumer<KeyType, ValueType>
    ) {
        whileEqualPrefix(
            context,
            DbNullKey(),
            keyInstance,
            valueInstance,
            KeyValuePairVisitor { k: KeyType, v: ValueType ->
                visitor.accept(k, v)
                true
            })
    }

    /**
     * This method is used mainly from other iterator methods to iterate over column family entries,
     * which are prefixed with column family key.
     */
    private fun <KeyType : DbKey?, ValueType : DbValue?> whileEqualPrefix(
        context: TransactionContext,
        keyInstance: KeyType,
        valueInstance: ValueType,
        visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileEqualPrefix(context, DbNullKey(), keyInstance, valueInstance, visitor)
    }

    /**
     * NOTE: it doesn't seem possible in Java RocksDB to set a flexible prefix extractor on iterators
     * at the moment, so using prefixes seem to be mostly related to skipping files that do not
     * contain keys with the given prefix (which is useful anyway), but it will still iterate over all
     * keys contained in those files, so we still need to make sure the key actually matches the
     * prefix.
     *
     *
     * While iterating over subsequent keys we have to validate it.
     */
    private fun <KeyType : DbKey?, ValueType : DbValue?> whileEqualPrefix(
        context: TransactionContext,
        prefix: DbKey?,
        keyInstance: KeyType,
        valueInstance: ValueType,
        visitor: KeyValuePairVisitor<KeyType, ValueType>
    ) {
        whileWithPrefix(
            context = context,
            prefix = prefix,
            keyInstance = keyInstance,
            valueInstance = valueInstance,
            visitor = visitor,
            predicate = { prefixKey, prefixLength, key ->
                startsWith(
                    prefixKey,
                    0,
                    prefixLength,
                    key,
                    0,
                    key.size
                )
            }
        )
    }

    private fun <KeyType : DbKey?, ValueType : DbValue?> whileWithPrefix(
        context: TransactionContext,
        prefix: DbKey?,
        keyInstance: KeyType,
        valueInstance: ValueType,
        visitor: KeyValuePairVisitor<KeyType, ValueType>,
        startAtKey: DbKey? = null,
        predicate: (ByteArray, Int, ByteArray) -> Boolean = ::startsWithPrefix
    ) {
        columnFamilyContext.withPrefixKey(prefix!!) { prefixKey: ByteArray?, prefixLength: Int ->
            columnFamilyContext.withPrefixKey(startAtKey ?: prefix) { seekKey: ByteArray?, seekLength: Int ->
                ensureInOpenTransaction(
                    context
                ) { transaction ->

                    transaction.newIterator().seek(seekKey!!, seekLength).iterate().forEach {

                        val keyBytes = it.key.byteArray

                        val shouldVisit = predicate.invoke(prefixKey!!, prefixLength, keyBytes)
                        if (!shouldVisit) {
                            return@ensureInOpenTransaction
                        }

                        val shouldVisitNext = visit(keyInstance, valueInstance, visitor, it)
                        if (!shouldVisitNext) {
                            return@ensureInOpenTransaction
                        }
                    }
                }
            }
        }
    }

    private fun <KeyType : DbKey?> whileWithPrefix(
        context: TransactionContext,
        prefix: DbKey?,
        keyInstance: KeyType,
        visitor: KeyVisitor<KeyType>,
        startAtKey: DbKey? = null,
        predicate: (ByteArray, Int, ByteArray) -> Boolean = ::startsWithPrefix
    ) {
        whileWithPrefix(
            context = context,
            prefix = prefix,
            keyInstance = keyInstance,
            valueInstance = valueInstance,
            visitor = KeyValuePairVisitor { key, _ -> visitor.visit(key) },
            startAtKey = startAtKey,
            predicate = predicate
        )
    }

    private fun <KeyType : DbKey?, ValueType : DbValue?> visit(
        keyInstance: KeyType,
        valueInstance: ValueType,
        iteratorConsumer: KeyValuePairVisitor<KeyType, ValueType>,
        entry: MutableMap.MutableEntry<Bytes, Bytes>
    ): Boolean {
        val keyBytes = entry.key
        columnFamilyContext.wrapKeyView(keyBytes.byteArray)
        columnFamilyContext.wrapValueView(entry.value.byteArray)
        val keyViewBuffer: DirectBuffer = columnFamilyContext.keyView!!
        keyInstance!!.wrap(keyViewBuffer, 0, keyViewBuffer.capacity())
        val valueViewBuffer: DirectBuffer = columnFamilyContext.valueView!!
        valueInstance!!.wrap(valueViewBuffer, 0, valueViewBuffer.capacity())
        return iteratorConsumer.visit(keyInstance, valueInstance)
    }

    override fun whileTrue(startAtKey: KeyType?, visitor: KeyValuePairVisitor<KeyType, ValueType>) {
        startAtKey?.let {
            whileWithPrefix(
                context = context,
                prefix = DbNullKey(),
                keyInstance = keyInstance,
                valueInstance = valueInstance,
                visitor = visitor,
                startAtKey = it
            )
        } ?: whileTrue(visitor)
    }

    private fun startsWithPrefix(prefixKey: ByteArray, prefixLength: Int, key: ByteArray): Boolean {
        return startsWith(
            prefixKey,
            0,
            prefixLength,
            key,
            0,
            key.size
        )
    }
}
