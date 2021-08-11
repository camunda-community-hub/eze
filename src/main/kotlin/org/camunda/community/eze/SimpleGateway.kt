package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.MessageIntent
import io.camunda.zeebe.util.buffer.BufferWriter
import io.grpc.stub.StreamObserver
import java.util.concurrent.atomic.AtomicLong

// BE AWARE THIS CLASS IS NOT THREAD SAFE
// FOR SIMPLICITY WE DON'T SUPPORT COMMAND ARRIVE IN PARALLEL
class SimpleGateway(private val writer: LogStreamRecordWriter) : GatewayGrpc.GatewayImplBase() {

    private val responseObserverMap = mutableMapOf<Long, StreamObserver<*>>()
    private val recordMetadata = RecordMetadata()
    private val requestIdGenerator = AtomicLong()

    private fun writeCommandWithKey(key : Long, metadata: RecordMetadata, bufferWriter : BufferWriter) {
        writer.reset()

        writer
            .key(key)
            .metadataWriter(metadata)
            .valueWriter(bufferWriter)
            .tryWrite()
    }

    private fun writeCommandWithoutKey(metadata: RecordMetadata, bufferWriter : BufferWriter) {
        writer.reset()

        writer
            .keyNull()
            .metadataWriter(metadata)
            .valueWriter(bufferWriter)
            .tryWrite()
    }

    fun responseCallback(requestId : Long, response: GeneratedMessageV3) {
        val streamObserver = responseObserverMap.remove(requestId) as StreamObserver<GeneratedMessageV3>
        streamObserver.onNext(response)
        streamObserver.onCompleted()
    }

    override fun publishMessage(
        request: GatewayOuterClass.PublishMessageRequest?,
        responseObserver: StreamObserver<GatewayOuterClass.PublishMessageResponse>?
    ) {
        val currentRequestId = requestIdGenerator.incrementAndGet()
        responseObserverMap[currentRequestId] = responseObserver!!

        recordMetadata.reset()
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.MESSAGE)
            .intent(MessageIntent.PUBLISH)
            .requestStreamId(1) // partition id
            .requestId(currentRequestId)

        val messageRecord = MessageRecord()

        val messageRequest = request!!

        messageRecord.correlationKey = messageRequest.correlationKey
        messageRecord.messageId = messageRequest.messageId
        messageRecord.name = messageRequest.name
        messageRecord.timeToLive = messageRequest.timeToLive
        // messageRecord.variables = messageRequest.variables // TODO support variables

        writeCommandWithoutKey(recordMetadata, messageRecord)
    }
}
