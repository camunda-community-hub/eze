/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.log.LogAppendEntry
import io.camunda.zeebe.logstreams.log.LogStreamWriter
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.stream.api.InterPartitionCommandSender
import java.util.function.Supplier

class SinglePartitionCommandSender(
    private val writerLookUp: (Int) -> LogStreamWriter
) : InterPartitionCommandSender {

    private fun withRecordWriter(
        receiverPartitionId: Int,
        writer: Supplier<LogAppendEntry>
    ) {
        val recordWriter = writerLookUp(receiverPartitionId)
        val entry = writer.get()
        recordWriter.tryWrite(entry)
    }

    override fun sendCommand(
        receiverPartitionId: Int,
        valueType: ValueType?,
        intent: Intent?,
        command: UnifiedRecordValue?
    ) {
        withRecordWriter(receiverPartitionId) {
            val recordMetadata =
                RecordMetadata()
                    .recordType(RecordType.COMMAND)
                    .valueType(valueType)
                    .intent(intent)

            LogAppendEntry.of(
                recordMetadata,
                command
            )
        }
    }

    override fun sendCommand(
        receiverPartitionId: Int,
        valueType: ValueType?,
        intent: Intent?,
        recordKey: Long,
        command: UnifiedRecordValue?
    ) {
        withRecordWriter(receiverPartitionId) {
            val recordMetadata =
                RecordMetadata()
                    .recordType(RecordType.COMMAND)
                    .valueType(valueType)
                    .intent(intent)

            LogAppendEntry.of(
                recordKey,
                recordMetadata,
                command
            )
        }
    }
}
