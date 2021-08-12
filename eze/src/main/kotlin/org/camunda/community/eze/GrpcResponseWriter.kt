/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import com.google.rpc.Code
import com.google.rpc.Status
import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord
import io.camunda.zeebe.protocol.impl.record.value.incident.IncidentRecord
import io.camunda.zeebe.protocol.impl.record.value.job.JobBatchRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceResultRecord
import io.camunda.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RejectionType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.intent.JobIntent
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.DirectBuffer
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.UnsafeBuffer

class GrpcResponseWriter(
    val responseCallback: (requestId: Long, response: GeneratedMessageV3) -> Unit,
    val errorCallback: (requestId: Long, error: Status) -> Unit
) :
    CommandResponseWriter {

    private var partitionId: Int = -1
    private var key: Long = -1
    private var intent: Intent = Intent.UNKNOWN
    private var recordType: RecordType = RecordType.NULL_VAL
    private var valueType: ValueType = ValueType.NULL_VAL
    private var rejectionType: RejectionType = RejectionType.NULL_VAL
    private var rejectionReason: String = ""

    private var valueBufferView: DirectBuffer = UnsafeBuffer()
    private var valueBuffer: MutableDirectBuffer = ExpandableArrayBuffer()


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
        value.write(valueBuffer, 0)
        valueBufferView.wrap(valueBuffer, 0, value.length)
        return this
    }

    override fun tryWriteResponse(requestStreamId: Int, requestId: Long): Boolean {

        if (rejectionType != RejectionType.NULL_VAL) {
            val rejectionResponse = createRejectionResponse()
            errorCallback(requestId, rejectionResponse)
            return true
        }

        val response: GeneratedMessageV3 = when (valueType) {
            ValueType.DEPLOYMENT -> createDeployResponse()
            ValueType.PROCESS_INSTANCE_CREATION -> createProcessInstanceResponse()
            ValueType.PROCESS_INSTANCE_RESULT -> createProcessInstanceWithResultResponse()
            ValueType.PROCESS_INSTANCE -> createCancelInstanceResponse()
            ValueType.INCIDENT -> createResolveIncidentResponse()
            ValueType.VARIABLE_DOCUMENT -> createSetVariablesResponse()
            ValueType.MESSAGE -> createMessageResponse()
            ValueType.JOB_BATCH -> createJobBatchResponse()
            ValueType.JOB -> when (intent) {
                JobIntent.COMPLETED -> createCompleteJobResponse()
                JobIntent.FAILED -> createFailJobResponse()
                JobIntent.ERROR_THROWN -> createJobThrowErrorResponse()
                JobIntent.RETRIES_UPDATED -> createJobUpdateRetriesResponse()
                else -> TODO("not support job command '$intent'")
            }
            else -> TODO("not supported command '$valueType'")
        }

        responseCallback(requestId, response)
        return true
    }

    private fun createResolveIncidentResponse(): GatewayOuterClass.ResolveIncidentResponse {
        val incident = IncidentRecord()
        incident.wrap(valueBufferView)

        return GatewayOuterClass.ResolveIncidentResponse.newBuilder().build()
    }

    private fun createDeployResponse(): GatewayOuterClass.DeployProcessResponse {
        val deployment = DeploymentRecord()
        deployment.wrap(valueBufferView)

        return GatewayOuterClass.DeployProcessResponse
            .newBuilder()
            .setKey(key)
            .addAllProcesses(
                deployment.processesMetadata().map {
                    GatewayOuterClass.ProcessMetadata.newBuilder()
                        .setProcessDefinitionKey(it.processDefinitionKey)
                        .setBpmnProcessId(it.bpmnProcessId)
                        .setVersion(it.version)
                        .setResourceName(it.resourceName)
                        .build()
                }
            ).build()
    }

    private fun createProcessInstanceResponse(): GatewayOuterClass.CreateProcessInstanceResponse {
        val processInstance = ProcessInstanceCreationRecord()
        processInstance.wrap(valueBufferView)

        return GatewayOuterClass.CreateProcessInstanceResponse.newBuilder()
            .setProcessInstanceKey(processInstance.processInstanceKey)
            .setProcessDefinitionKey(processInstance.processDefinitionKey)
            .setBpmnProcessId(processInstance.bpmnProcessId)
            .setVersion(processInstance.version)
            .build()
    }

    private fun createProcessInstanceWithResultResponse(): GatewayOuterClass.CreateProcessInstanceWithResultResponse {
        val processInstanceResult = ProcessInstanceResultRecord()
        processInstanceResult.wrap(valueBufferView)

        return GatewayOuterClass.CreateProcessInstanceWithResultResponse.newBuilder()
            .setProcessInstanceKey(processInstanceResult.processInstanceKey)
            .setProcessDefinitionKey(processInstanceResult.processDefinitionKey)
            .setBpmnProcessId(processInstanceResult.bpmnProcessId)
            .setVersion(processInstanceResult.version)
            .setVariables(MsgPackConverter.convertToJson(processInstanceResult.variablesBuffer))
            .build()
    }

    private fun createSetVariablesResponse(): GatewayOuterClass.SetVariablesResponse {
        val variableDocumentRecord = VariableDocumentRecord()
        variableDocumentRecord.wrap(valueBufferView)

        return GatewayOuterClass.SetVariablesResponse.newBuilder()
            .setKey(key)
            .build()
    }


    private fun createCancelInstanceResponse(): GatewayOuterClass.CancelProcessInstanceResponse {
        return GatewayOuterClass.CancelProcessInstanceResponse.newBuilder()
            .build()
    }

    private fun createMessageResponse(): GatewayOuterClass.PublishMessageResponse {
        return GatewayOuterClass.PublishMessageResponse
            .newBuilder()
            .setKey(key).build()
    }

    private fun createJobBatchResponse(): GatewayOuterClass.ActivateJobsResponse {
        val jobBatch = JobBatchRecord()
        jobBatch.wrap(valueBufferView)

        val jobsWithKeys = jobBatch.jobKeys().map { it.value }
            .zip(jobBatch.jobs())

        return GatewayOuterClass.ActivateJobsResponse.newBuilder()
            .addAllJobs(
                jobsWithKeys.map { (jobKey, job) ->
                    GatewayOuterClass.ActivatedJob.newBuilder()
                        .setKey(jobKey)
                        .setType(job.type)
                        .setRetries(job.retries)
                        .setWorker(job.worker)
                        .setDeadline(job.deadline)
                        .setProcessDefinitionKey(job.processDefinitionKey)
                        .setBpmnProcessId(job.bpmnProcessId)
                        .setProcessDefinitionVersion(job.processDefinitionVersion)
                        .setProcessInstanceKey(job.processInstanceKey)
                        .setElementId(job.elementId)
                        .setElementInstanceKey(job.elementInstanceKey)
                        .setCustomHeaders(MsgPackConverter.convertToJson(job.customHeadersBuffer))
                        .setVariables(MsgPackConverter.convertToJson(job.variablesBuffer))
                        .build()
                }
            )
            .build()
    }

    private fun createCompleteJobResponse(): GatewayOuterClass.CompleteJobResponse {
        return GatewayOuterClass.CompleteJobResponse.newBuilder()
            .build()
    }

    private fun createFailJobResponse(): GatewayOuterClass.FailJobResponse {
        return GatewayOuterClass.FailJobResponse.newBuilder()
            .build()
    }

    private fun createJobThrowErrorResponse(): GatewayOuterClass.ThrowErrorResponse {
        return GatewayOuterClass.ThrowErrorResponse.newBuilder()
            .build()
    }

    private fun createJobUpdateRetriesResponse(): GatewayOuterClass.UpdateJobRetriesResponse {
        return GatewayOuterClass.UpdateJobRetriesResponse.newBuilder()
            .build()
    }

    private fun createRejectionResponse(): Status {
        val statusCode = when (rejectionType) {
            RejectionType.INVALID_ARGUMENT -> Code.INVALID_ARGUMENT_VALUE
            RejectionType.NOT_FOUND -> Code.NOT_FOUND_VALUE
            RejectionType.ALREADY_EXISTS -> Code.ALREADY_EXISTS_VALUE
            RejectionType.INVALID_STATE -> Code.FAILED_PRECONDITION_VALUE
            RejectionType.PROCESSING_ERROR -> Code.INTERNAL_VALUE
            else -> Code.UNKNOWN_VALUE
        }

        return Status.newBuilder()
            .setMessage("Command '$intent' rejected with code '$rejectionType': $rejectionReason")
            .setCode(statusCode)
            .build()
    }
}
