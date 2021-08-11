package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RejectionType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.intent.MessageIntent
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.DirectBuffer

class GrpcResponseWriter(val responseCallback: (requestId: Long, response: GeneratedMessageV3) -> Unit) :
    CommandResponseWriter {

    private var partitionId: Int = -1
    private var key: Long = -1
    private var intent: Intent = Intent.UNKNOWN
    private var recordType: RecordType = RecordType.NULL_VAL
    private var valueType: ValueType = ValueType.NULL_VAL
    private var rejectionType: RejectionType = RejectionType.NULL_VAL
    private var rejectionReason: String = ""
    private var value: BufferWriter? = null

    override fun partitionId(partitionId: Int): CommandResponseWriter {
        this.partitionId = partitionId
        return this
    }

    override fun key(key: Long): CommandResponseWriter {
        this.key = key
        return this
    }

    override fun intent(intent: Intent): CommandResponseWriter {
        this.intent = intent
        return this
    }

    override fun recordType(type: RecordType): CommandResponseWriter {
        this.recordType = recordType
        return this
    }

    override fun valueType(valueType: ValueType): CommandResponseWriter {
        this.valueType = valueType
        return this
    }

    override fun rejectionType(rejectionType: RejectionType): CommandResponseWriter {
       this.rejectionType = rejectionType
        return this
    }

    override fun rejectionReason(rejectionReason: DirectBuffer): CommandResponseWriter {
        this.rejectionReason = BufferUtil.bufferAsString(rejectionReason)
        return this
    }

    override fun valueWriter(value: BufferWriter): CommandResponseWriter {
        // TODO store value
        return this
    }

    override fun tryWriteResponse(requestStreamId: Int, requestId: Long): Boolean {

        val response: GeneratedMessageV3 = when (valueType) {
            ValueType.MESSAGE -> when (intent) {
                MessageIntent.PUBLISHED -> GatewayOuterClass.PublishMessageResponse.newBuilder()
                    .setKey(key).build()
                else -> TODO()
            }
            else -> TODO()
        }

        responseCallback(requestId, response)

        return true
    }
}
