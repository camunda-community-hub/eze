package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent
import io.camunda.zeebe.protocol.record.intent.MessageIntent
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceCreationIntent
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent
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

    override fun publishMessage(
        messageRequest: GatewayOuterClass.PublishMessageRequest,
        responseObserver: StreamObserver<GatewayOuterClass.PublishMessageResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.MESSAGE)
            .intent(MessageIntent.PUBLISH)

        val messageRecord = MessageRecord()

        messageRecord.correlationKey = messageRequest.correlationKey
        messageRecord.messageId = messageRequest.messageId
        messageRecord.name = messageRequest.name
        messageRecord.timeToLive = messageRequest.timeToLive
        // messageRecord.variables = messageRequest.variables // TODO support variables

        writeCommandWithoutKey(recordMetadata, messageRecord)
    }

    override fun deployProcess(
        request: GatewayOuterClass.DeployProcessRequest,
        responseObserver: StreamObserver<GatewayOuterClass.DeployProcessResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.DEPLOYMENT)
            .intent(DeploymentIntent.CREATE)

        val deploymentRecord = DeploymentRecord()
        val resources = deploymentRecord.resources()

        request.processesList.forEach {
            processRequestObject ->
                resources
                    .add()
                    .setResourceName(processRequestObject.name)
                    .setResource(processRequestObject.definition.toByteArray())
        }

        writeCommandWithoutKey(recordMetadata, deploymentRecord)
    }

    override fun createProcessInstance(
        request: GatewayOuterClass.CreateProcessInstanceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CreateProcessInstanceResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.PROCESS_INSTANCE)
            .intent(ProcessInstanceCreationIntent.CREATE)

        val processInstanceCreationRecord = ProcessInstanceCreationRecord()
        processInstanceCreationRecord.bpmnProcessId = request.bpmnProcessId
        processInstanceCreationRecord.processDefinitionKey = request.processDefinitionKey
        processInstanceCreationRecord.version = request.version
//        processInstanceCreationRecord.variablesBuffer = request TODO handle variables

        writeCommandWithoutKey(recordMetadata, processInstanceCreationRecord)
    }

    private fun prepareRecordMetadata() : RecordMetadata {
        return recordMetadata.reset()
            .recordType(RecordType.COMMAND)
            .requestStreamId(1) // partition id
    }

    private fun registerNewRequest(responseObserver: StreamObserver<*>) : Long {
        val currentRequestId = requestIdGenerator.incrementAndGet()
        responseObserverMap[currentRequestId] = responseObserver
        return currentRequestId
    }

    fun responseCallback(requestId : Long, response: GeneratedMessageV3) {
        val streamObserver = responseObserverMap.remove(requestId) as StreamObserver<GeneratedMessageV3>
        streamObserver.onNext(response)
        streamObserver.onCompleted()
    }
}
