package org.camunda.community.eze.engine

import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.stream.api.InterPartitionCommandSender
import io.camunda.zeebe.stream.api.records.TypedRecord
import org.camunda.community.eze.records.RecordWrapper
import java.util.function.Consumer

class SinglePartitionCommandSender(private val writer: Consumer<TypedRecord<UnifiedRecordValue>>) :
    InterPartitionCommandSender {

    override fun sendCommand(
        receiverPartitionId: Int,
        valueType: ValueType?,
        intent: Intent?,
        command: UnifiedRecordValue?
    ) {
        val recordMetadata = RecordMetadata()
        recordMetadata.recordType(RecordType.COMMAND)
            .valueType(valueType)
            .intent(intent)

        writer.accept(RecordWrapper(command!!, recordMetadata, -1))
    }

    override fun sendCommand(
        receiverPartitionId: Int,
        valueType: ValueType?,
        intent: Intent?,
        recordKey: Long?,
        command: UnifiedRecordValue?
    ) {
        val recordMetadata = RecordMetadata()
        recordMetadata.recordType(RecordType.COMMAND)
            .valueType(valueType)
            .intent(intent)

        writer.accept(RecordWrapper(command!!, recordMetadata, recordKey!!))
    }
}
