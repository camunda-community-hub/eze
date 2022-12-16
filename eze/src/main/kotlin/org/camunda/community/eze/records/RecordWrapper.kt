/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.records

import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RejectionType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.stream.api.records.TypedRecord
import io.camunda.zeebe.stream.impl.records.RecordBatchEntry

class RecordWrapper(
    private val recordValue : UnifiedRecordValue,
    private val recordMetadata: RecordMetadata,
    private val key : Long
) : TypedRecord<UnifiedRecordValue> {
    override fun getPosition(): Long {
        TODO("Not yet implemented")
    }

    companion object {
        fun convert(recordBatchEntry: RecordBatchEntry) : TypedRecord<UnifiedRecordValue> {
            return RecordWrapper(recordBatchEntry.recordValue(), recordMetadata = recordBatchEntry.recordMetadata(), recordBatchEntry.key())
        }
    }

    override fun getSourceRecordPosition(): Long {
        return -1;
    }

    override fun getKey(): Long {
        return key;
    }

    override fun getTimestamp(): Long {

        TODO("Not yet implemented")
    }

    override fun getIntent(): Intent {
        return recordMetadata.intent
    }

    override fun getPartitionId(): Int {
        return 1;
    }

    override fun getRecordType(): RecordType {
        return recordMetadata.recordType
    }

    override fun getRejectionType(): RejectionType {
        return recordMetadata.rejectionType
    }

    override fun getRejectionReason(): String {
        return recordMetadata.rejectionReason
    }

    override fun getBrokerVersion(): String {
        return recordMetadata.brokerVersion.toString()
    }

    override fun getValueType(): ValueType {
        return recordMetadata.valueType
    }

    override fun getValue(): UnifiedRecordValue {
        return recordValue
    }

    override fun getRequestStreamId(): Int {
        return recordMetadata.requestStreamId
    }

    override fun getRequestId(): Long {
        return recordMetadata.requestId
    }

    override fun getLength(): Int {
        return recordMetadata.length + recordValue.length
    }

    override fun toString(): String {
        return "RecordWrapper(recordValue=$recordValue, recordMetadata=$recordMetadata, key=$key)"
    }
}
