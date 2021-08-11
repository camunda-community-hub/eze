package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RejectionType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.DirectBuffer

class GrpcResponseWriter: CommandResponseWriter {

    override fun partitionId(partitionId: Int): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun key(key: Long): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun intent(intent: Intent?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun recordType(type: RecordType?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun valueType(valueType: ValueType?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun rejectionType(rejectionType: RejectionType?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun rejectionReason(rejectionReason: DirectBuffer?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun valueWriter(value: BufferWriter?): CommandResponseWriter {
        TODO("Not yet implemented")
    }

    override fun tryWriteResponse(requestStreamId: Int, requestId: Long): Boolean {
        TODO("Not yet implemented")
    }
}
