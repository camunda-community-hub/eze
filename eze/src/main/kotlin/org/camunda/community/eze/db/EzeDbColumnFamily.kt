package org.camunda.community.eze.db

import io.camunda.zeebe.db.*
import io.camunda.zeebe.util.buffer.BufferUtil.startsWith
import org.agrona.DirectBuffer
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer
import java.util.function.Consumer


class EzeDbColumnFamily<ColumnFamilyNames : Enum<ColumnFamilyNames>,
        KeyType : DbKey,
        ValueType : DbValue>(
    val database: TreeMap<Bytes, Bytes>,
    val columnFamily: ColumnFamilyNames,
    val context: TransactionContext,
    val keyInstance: KeyType,
    val valueInstance: ValueType
) : ColumnFamily<KeyType, ValueType> {

    private val columnFamilyContext = ColumnFamilyContext(columnFamily.ordinal.toLong())

    private fun ensureInOpenTransaction(
        context: TransactionContext,
        operation: TransactionConsumer
    ) {
        context.runInTransaction {
            operation.run(context.currentTransaction as EzeDbTransaction)
        }
    }

    override fun put(key: KeyType, value: ValueType) {
        ensureInOpenTransaction(
            context
        ) { transaction ->
            columnFamilyContext.writeKey(key)
            columnFamilyContext.writeValue(value)

            transaction.put(
                columnFamilyContext.keyBufferArray,
                columnFamilyContext.keyLength,
                columnFamilyContext.valueBufferArray,
                value.length
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

    override fun delete(key: KeyType) {
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
        columnFamilyContext.withPrefixKey(
            prefix!!
        ) { prefixKey: ByteArray?, prefixLength: Int ->
            ensureInOpenTransaction(
                context
            ) { transaction ->

                transaction.newIterator().seek(prefixKey!!, prefixLength).iterate().forEach {

                    val keyBytes = it.key.byteArray
                    if (!startsWith(prefixKey, 0, prefixLength, keyBytes, 0, keyBytes.size)) {
                        return@forEach
                    }
                    val shouldVisitNext = visit(keyInstance, valueInstance, visitor, it)

                    if (!shouldVisitNext) {
                        return@ensureInOpenTransaction
                    }


                }
            }
        }
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
}
