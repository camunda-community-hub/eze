/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.grpc

import com.google.protobuf.GeneratedMessageV3
import com.google.rpc.Status
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.logstreams.log.LogAppendEntry
import io.camunda.zeebe.logstreams.log.LogStreamWriter
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.protocol.impl.record.value.decision.DecisionEvaluationRecord
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord
import io.camunda.zeebe.protocol.impl.record.value.incident.IncidentRecord
import io.camunda.zeebe.protocol.impl.record.value.job.JobBatchRecord
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.*
import io.camunda.zeebe.protocol.impl.record.value.resource.ResourceDeletionRecord
import io.camunda.zeebe.protocol.impl.record.value.signal.SignalRecord
import io.camunda.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.*
import io.camunda.zeebe.protocol.record.value.VariableDocumentUpdateSemantic
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferUtil.wrapString
import io.grpc.protobuf.StatusProto
import io.grpc.stub.StreamObserver
import org.agrona.DirectBuffer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class GrpcToLogStreamGateway(
    private val writer: LogStreamWriter
) : GatewayGrpc.GatewayImplBase(), AutoCloseable {

    private val executor = Executors.newSingleThreadExecutor()

    private val responseObserverMap = mutableMapOf<Long, StreamObserver<*>>()
    private val responseTypeMap = mutableMapOf<Long, Class<out GeneratedMessageV3>>()

    private val requestIdGenerator = AtomicLong()

    private val versionInfo = getVersionInfo()

    private fun writeCommandWithKey(
        key: Long,
        metadata: RecordMetadata,
        recordValue: UnifiedRecordValue
    ) {
        writer.tryWrite(
            LogAppendEntry.of(
                key,
                metadata,
                recordValue
            )
        )
    }

    private fun writeCommandWithoutKey(metadata: RecordMetadata, recordValue: UnifiedRecordValue) {
        writer.tryWrite(
            LogAppendEntry.of(
                metadata,
                recordValue
            )
        )
    }


    override fun publishMessage(
        messageRequest: GatewayOuterClass.PublishMessageRequest,
        responseObserver: StreamObserver<GatewayOuterClass.PublishMessageResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.MESSAGE)
                .intent(MessageIntent.PUBLISH)

            val messageRecord = MessageRecord()

            messageRecord.correlationKey = messageRequest.correlationKey
            messageRecord.messageId = messageRequest.messageId
            messageRecord.name = messageRequest.name
            messageRecord.timeToLive = messageRequest.timeToLive
            setVariablesAsMessagePack(messageRequest.variables, messageRecord::setVariables)

            writeCommandWithoutKey(recordMetadata, messageRecord)
        }
    }

    override fun deployProcess(
        request: GatewayOuterClass.DeployProcessRequest,
        responseObserver: StreamObserver<GatewayOuterClass.DeployProcessResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
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
    }

    override fun deployResource(
        request: GatewayOuterClass.DeployResourceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.DeployResourceResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.DEPLOYMENT)
                .intent(DeploymentIntent.CREATE)

            val deploymentRecord = DeploymentRecord()
            val resources = deploymentRecord.resources()

            request.resourcesList.forEach { resourceRequestObject ->
                resources
                    .add()
                    .setResourceName(resourceRequestObject.name)
                    .setResource(resourceRequestObject.content.toByteArray())
            }

            writeCommandWithoutKey(recordMetadata, deploymentRecord)
        }
    }

    override fun createProcessInstance(
        request: GatewayOuterClass.CreateProcessInstanceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CreateProcessInstanceResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.PROCESS_INSTANCE_CREATION)
                .intent(ProcessInstanceCreationIntent.CREATE)

            val processInstanceCreationRecord = createProcessInstanceCreationRecord(request)
            writeCommandWithoutKey(recordMetadata, processInstanceCreationRecord)
        }
    }

    override fun createProcessInstanceWithResult(
        request: GatewayOuterClass.CreateProcessInstanceWithResultRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CreateProcessInstanceWithResultResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.PROCESS_INSTANCE_CREATION)
                .intent(ProcessInstanceCreationIntent.CREATE_WITH_AWAITING_RESULT)

            val processInstanceCreationRecord = createProcessInstanceCreationRecord(request.request)
            processInstanceCreationRecord.setFetchVariables(request.fetchVariablesList)

            writeCommandWithoutKey(recordMetadata, processInstanceCreationRecord)
        }
    }

    private fun createProcessInstanceCreationRecord(creationRequest: GatewayOuterClass.CreateProcessInstanceRequest): ProcessInstanceCreationRecord {
        val processInstanceCreationRecord = ProcessInstanceCreationRecord()

        processInstanceCreationRecord.bpmnProcessId = creationRequest.bpmnProcessId
        processInstanceCreationRecord.version = creationRequest.version
        processInstanceCreationRecord.processDefinitionKey = creationRequest.processDefinitionKey
        setVariablesAsMessagePack(
            creationRequest.variables,
            processInstanceCreationRecord::setVariables
        )

        creationRequest.startInstructionsList.forEach {
            val instruction = ProcessInstanceCreationStartInstruction()
            instruction.elementId = it.elementId
            processInstanceCreationRecord.addStartInstruction(instruction)
        }

        return processInstanceCreationRecord
    }

    override fun cancelProcessInstance(
        request: GatewayOuterClass.CancelProcessInstanceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CancelProcessInstanceResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.PROCESS_INSTANCE)
                .intent(ProcessInstanceIntent.CANCEL)

            val processInstanceRecord = ProcessInstanceRecord()

            processInstanceRecord.processInstanceKey = request.processInstanceKey

            writeCommandWithKey(request.processInstanceKey, recordMetadata, processInstanceRecord)
        }
    }

    override fun setVariables(
        request: GatewayOuterClass.SetVariablesRequest,
        responseObserver: StreamObserver<GatewayOuterClass.SetVariablesResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.VARIABLE_DOCUMENT)
                .intent(VariableDocumentIntent.UPDATE)

            val variableDocumentRecord = VariableDocumentRecord()
            setVariablesAsMessagePack(request.variables, variableDocumentRecord::setVariables)

            variableDocumentRecord.scopeKey = request.elementInstanceKey
            variableDocumentRecord.updateSemantics =
                if (request.local) VariableDocumentUpdateSemantic.LOCAL else VariableDocumentUpdateSemantic.PROPAGATE

            writeCommandWithoutKey(recordMetadata, variableDocumentRecord)
        }
    }

    override fun resolveIncident(
        request: GatewayOuterClass.ResolveIncidentRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ResolveIncidentResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.INCIDENT)
                .intent(IncidentIntent.RESOLVE)

            val incidentRecord = IncidentRecord()

            writeCommandWithKey(request.incidentKey, recordMetadata, incidentRecord)
        }
    }

    override fun activateJobs(
        request: GatewayOuterClass.ActivateJobsRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ActivateJobsResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
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
    }

    override fun completeJob(
        request: GatewayOuterClass.CompleteJobRequest,
        responseObserver: StreamObserver<GatewayOuterClass.CompleteJobResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.JOB)
                .intent(JobIntent.COMPLETE)

            val jobRecord = JobRecord()
            setVariablesAsMessagePack(request.variables, jobRecord::setVariables)

            writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
        }
    }

    override fun failJob(
        request: GatewayOuterClass.FailJobRequest,
        responseObserver: StreamObserver<GatewayOuterClass.FailJobResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.JOB)
                .intent(JobIntent.FAIL)

            val jobRecord = JobRecord()

            jobRecord.retries = request.retries
            jobRecord.errorMessage = request.errorMessage
            jobRecord.retryBackoff = request.retryBackOff

            writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
        }
    }

    override fun throwError(
        request: GatewayOuterClass.ThrowErrorRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ThrowErrorResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.JOB)
                .intent(JobIntent.THROW_ERROR)

            val jobRecord = JobRecord()

            jobRecord.setErrorCode(wrapString(request.errorCode))
            jobRecord.errorMessage = request.errorMessage

            writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
        }
    }

    override fun updateJobRetries(
        request: GatewayOuterClass.UpdateJobRetriesRequest,
        responseObserver: StreamObserver<GatewayOuterClass.UpdateJobRetriesResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.JOB)
                .intent(JobIntent.UPDATE_RETRIES)

            val jobRecord = JobRecord()
            jobRecord.retries = request.retries

            writeCommandWithKey(request.jobKey, recordMetadata, jobRecord)
        }
    }

    override fun modifyProcessInstance(
        request: GatewayOuterClass.ModifyProcessInstanceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.ModifyProcessInstanceResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.PROCESS_INSTANCE_MODIFICATION)
                .intent(ProcessInstanceModificationIntent.MODIFY)

            val modificationRecord = ProcessInstanceModificationRecord()
            modificationRecord.processInstanceKey = request.processInstanceKey

            request.activateInstructionsList.forEach {
                val instruction = ProcessInstanceModificationActivateInstruction()
                instruction.elementId = it.elementId
                instruction.ancestorScopeKey = it.ancestorElementInstanceKey

                it.variableInstructionsList.forEach {
                    val variableInstruction = ProcessInstanceModificationVariableInstruction()
                    variableInstruction.elementId = it.scopeId
                    setVariablesAsMessagePack(it.variables, variableInstruction::setVariables)

                    instruction.addVariableInstruction(variableInstruction)
                }

                modificationRecord.addActivateInstruction(instruction)
            }

            request.terminateInstructionsList.forEach {
                val instruction = ProcessInstanceModificationTerminateInstruction()
                instruction.elementInstanceKey = it.elementInstanceKey

                modificationRecord.addTerminateInstruction(instruction)
            }

            writeCommandWithKey(request.processInstanceKey, recordMetadata, modificationRecord)
        }
    }

    override fun evaluateDecision(
        request: GatewayOuterClass.EvaluateDecisionRequest,
        responseObserver: StreamObserver<GatewayOuterClass.EvaluateDecisionResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.DECISION_EVALUATION)
                .intent(DecisionEvaluationIntent.EVALUATE)

            val command = DecisionEvaluationRecord()

            request.decisionKey.takeIf { it > 0 }?.let { command.decisionKey = it }
            request.decisionId.takeIf { it.isNotEmpty() }?.let { command.decisionId = it }

            setVariablesAsMessagePack(request.variables, command::setVariables)

            writeCommandWithoutKey(recordMetadata, command)
        }
    }

    override fun deleteResource(
        request: GatewayOuterClass.DeleteResourceRequest,
        responseObserver: StreamObserver<GatewayOuterClass.DeleteResourceResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.RESOURCE_DELETION)
                .intent(ResourceDeletionIntent.DELETE)

            val command = ResourceDeletionRecord()
            command.resourceKey = request.resourceKey

            writeCommandWithoutKey(recordMetadata, command)
        }
    }

    override fun broadcastSignal(
        signalRequest: GatewayOuterClass.BroadcastSignalRequest,
        responseObserver: StreamObserver<GatewayOuterClass.BroadcastSignalResponse>
    ) {
        executor.submit {
            val requestId = registerNewRequest(responseObserver)

            val recordMetadata = prepareRecordMetadata()
                .requestId(requestId)
                .valueType(ValueType.SIGNAL)
                .intent(SignalIntent.BROADCAST)

            val signalRecord = SignalRecord()

            signalRecord.signalName = signalRequest.signalName
            setVariablesAsMessagePack(signalRequest.variables, signalRecord::setVariables)

            writeCommandWithoutKey(recordMetadata, signalRecord)
        }
    }

    override fun topology(
        request: GatewayOuterClass.TopologyRequest,
        responseObserver: StreamObserver<GatewayOuterClass.TopologyResponse>
    ) {
        val partition = GatewayOuterClass.Partition
            .newBuilder()
            .setHealth(GatewayOuterClass.Partition.PartitionBrokerHealth.HEALTHY)
            .setRole(GatewayOuterClass.Partition.PartitionBrokerRole.LEADER)
            .setPartitionId(1)
            .build()

        val brokerInfo = GatewayOuterClass.BrokerInfo
            .newBuilder()
            .addPartitions(partition)
            .setHost("0.0.0.0")
            .setPort(26500)
            .setVersion(versionInfo)
            .build()

        val topologyResponse = GatewayOuterClass.TopologyResponse
            .newBuilder()
            .addBrokers(brokerInfo)
            .setClusterSize(1)
            .setPartitionsCount(1)
            .setReplicationFactor(1)
            .setGatewayVersion(versionInfo)
            .build()

        responseObserver.onNext(topologyResponse)
        responseObserver.onCompleted()
    }

    private fun prepareRecordMetadata(): RecordMetadata {
        return RecordMetadata()
            .recordType(RecordType.COMMAND)
            .requestStreamId(1) // partition id
    }

    private fun setVariablesAsMessagePack(variablesJson: String, setter: (DirectBuffer) -> Unit) {
        variablesJson.takeIf { it.isNotEmpty() }
            ?.let { MsgPackConverter.convertToMsgPack(it) }
            ?.let { BufferUtil.wrapArray(it) }
            ?.let(setter)
    }

    private inline fun <reified T : GeneratedMessageV3> registerNewRequest(
        responseObserver: StreamObserver<T>
    ): Long {
        val currentRequestId = requestIdGenerator.incrementAndGet()
        responseObserverMap[currentRequestId] = responseObserver
        responseTypeMap[currentRequestId] = T::class.java
        return currentRequestId
    }

    fun responseCallback(requestId: Long, response: GeneratedMessageV3) {
        executor.submit {
            val streamObserver =
                responseObserverMap.remove(requestId) as StreamObserver<GeneratedMessageV3>
            streamObserver.onNext(response)
            streamObserver.onCompleted()
        }
    }

    fun errorCallback(requestId: Long, error: Status) {
        executor.submit {
            val streamObserver =
                responseObserverMap.remove(requestId) as StreamObserver<GeneratedMessageV3>
            streamObserver.onError(StatusProto.toStatusException(error))
        }
    }

    fun getExpectedResponseType(requestId: Long): Class<out GeneratedMessageV3>? {
        return responseTypeMap[requestId]
    }

    override fun close() {
        try {
            executor.shutdownNow()
            executor.awaitTermination(60, TimeUnit.SECONDS)
        } catch (ie: InterruptedException) {
            // TODO handle
        }
    }

    private fun getVersionInfo(): String {
        val ezeVersion = javaClass.`package`.implementationVersion ?: "dev"
        val zeebeVersion = RecordMetadata::class.java.`package`.implementationVersion ?: "dev"

        return "$ezeVersion ($zeebeVersion)"
    }
}
