package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord
import io.camunda.zeebe.protocol.impl.record.value.job.JobBatchRecord
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.*
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferUtil.wrapString
import io.camunda.zeebe.util.buffer.BufferWriter
import io.grpc.stub.StreamObserver
import org.agrona.concurrent.UnsafeBuffer
import java.util.concurrent.atomic.AtomicLong

// BE AWARE THIS CLASS IS NOT THREAD SAFE
// FOR SIMPLICITY WE DON'T SUPPORT COMMAND ARRIVE IN PARALLEL
class SimpleGateway(private val writer: LogStreamRecordWriter) : GatewayGrpc.GatewayImplBase() {

    private val responseObserverMap = mutableMapOf<Long, StreamObserver<*>>()
    private val recordMetadata = RecordMetadata()
    private val requestIdGenerator = AtomicLong()

    private fun writeCommandWithKey(
        key: Long,
        metadata: RecordMetadata,
        bufferWriter: BufferWriter
    ) {
        writer.reset()

        writer
            .key(key)
            .metadataWriter(metadata)
            .valueWriter(bufferWriter)
            .tryWrite()
    }

    private fun writeCommandWithoutKey(metadata: RecordMetadata, bufferWriter: BufferWriter) {
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
        messageRecord.setVariables(UnsafeBuffer(MsgPackConverter.convertToMsgPack(messageRecord.variables)))

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

        request.processesList.forEach { processRequestObject ->
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
            .valueType(ValueType.PROCESS_INSTANCE_CREATION)
            .intent(ProcessInstanceCreationIntent.CREATE)

        val processInstanceCreationRecord = createProcessInstanceCreationRecord(request)
        writeCommandWithoutKey(recordMetadata, processInstanceCreationRecord)
    }

    override fun createProcessInstanceWithResult(
        request: GatewayOuterClass.CreateProcessInstanceWithResultRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CreateProcessInstanceWithResultResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.PROCESS_INSTANCE_CREATION)
            .intent(ProcessInstanceCreationIntent.CREATE_WITH_AWAITING_RESULT)

        val processInstanceCreationRecord = createProcessInstanceCreationRecord(request.request)
        processInstanceCreationRecord.setFetchVariables(request.fetchVariablesList)

        writeCommandWithoutKey(recordMetadata, processInstanceCreationRecord)
    }

    private fun createProcessInstanceCreationRecord(creationRequest: GatewayOuterClass.CreateProcessInstanceRequest): ProcessInstanceCreationRecord {
        val processInstanceCreationRecord = ProcessInstanceCreationRecord()

        processInstanceCreationRecord.bpmnProcessId = creationRequest.bpmnProcessId
        processInstanceCreationRecord.version = creationRequest.version
        processInstanceCreationRecord.processDefinitionKey = creationRequest.processDefinitionKey

        creationRequest.variables.takeIf { it.isNotEmpty() }?.let {
            val variables = BufferUtil.wrapArray(MsgPackConverter.convertToMsgPack(it))
            processInstanceCreationRecord.setVariables(variables)
        }
        return processInstanceCreationRecord
    }

    override fun cancelProcessInstance(
        request: GatewayOuterClass.CancelProcessInstanceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CancelProcessInstanceResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.PROCESS_INSTANCE)
            .intent(ProcessInstanceIntent.CANCEL)

        val processInstanceRecord = ProcessInstanceRecord()

        processInstanceRecord.processInstanceKey = request.processInstanceKey

        writeCommandWithKey(request.processInstanceKey, recordMetadata, processInstanceRecord)
    }

    override fun activateJobs(
        request: GatewayOuterClass.ActivateJobsRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ActivateJobsResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.JOB_BATCH)
            .intent(JobBatchIntent.ACTIVATE)

        val jobBatchRecord = JobBatchRecord()

        jobBatchRecord.type = request.type
        jobBatchRecord.worker = request.worker
        jobBatchRecord.timeout = request.timeout
        jobBatchRecord.maxJobsToActivate = request.maxJobsToActivate

        writeCommandWithoutKey(recordMetadata, jobBatchRecord)
    }

    override fun completeJob(
        request: GatewayOuterClass.CompleteJobRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CompleteJobResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.JOB)
            .intent(JobIntent.COMPLETE)

        val jobRecord = JobRecord()

        request.variables.takeIf { it.isNotEmpty() }?.let {
            val variables = BufferUtil.wrapArray(MsgPackConverter.convertToMsgPack(it))
            jobRecord.setVariables(variables)
        }

        writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
    }

    override fun failJob(
        request: GatewayOuterClass.FailJobRequest,
        responseObserver: StreamObserver<GatewayOuterClass.FailJobResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.JOB)
            .intent(JobIntent.FAIL)

        val jobRecord = JobRecord()

        jobRecord.retries = request.retries
        jobRecord.errorMessage = request.errorMessage

        writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
    }

    override fun throwError(
        request: GatewayOuterClass.ThrowErrorRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ThrowErrorResponse>
    ) {
        val requestId = registerNewRequest(responseObserver)

        prepareRecordMetadata()
            .requestId(requestId)
            .valueType(ValueType.JOB)
            .intent(JobIntent.THROW_ERROR)


        val jobRecord = JobRecord()

        jobRecord.setErrorCode(wrapString(request.errorCode))
        jobRecord.errorMessage = request.errorMessage

        writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
    }

    private fun prepareRecordMetadata(): RecordMetadata {
        return recordMetadata.reset()
            .recordType(RecordType.COMMAND)
            .requestStreamId(1) // partition id
    }

    private fun registerNewRequest(responseObserver: StreamObserver<*>): Long {
        val currentRequestId = requestIdGenerator.incrementAndGet()
        responseObserverMap[currentRequestId] = responseObserver
        return currentRequestId
    }

    fun responseCallback(requestId: Long, response: GeneratedMessageV3) {
        val streamObserver =
            responseObserverMap.remove(requestId) as StreamObserver<GeneratedMessageV3>
        streamObserver.onNext(response)
        streamObserver.onCompleted()
    }
}
