package org.camunda.community.eze

import com.google.protobuf.GeneratedMessageV3
import io.camunda.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceResultRecord
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RejectionType
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.util.buffer.BufferUtil
import io.camunda.zeebe.util.buffer.BufferWriter
import org.agrona.DirectBuffer
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.UnsafeBuffer

class GrpcResponseWriter(val responseCallback: (requestId: Long, response: GeneratedMessageV3) -> Unit) :
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

        val response: GeneratedMessageV3 = when (valueType) {
            ValueType.DEPLOYMENT -> createDeployResponse()
            ValueType.PROCESS_INSTANCE -> createProcessInstanceResponse()
            ValueType.PROCESS_INSTANCE_RESULT -> createProcessInstanceWithResultResponse()
            ValueType.MESSAGE -> createMessageResponse()
            else -> TODO("implement other types")
        }

        responseCallback(requestId, response)

        return true
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
        val processInstance = ProcessInstanceRecord()
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
            .setVariables(BufferUtil.bufferAsString(processInstanceResult.variablesBuffer))
            .build()
    }

    private fun createMessageResponse(): GatewayOuterClass.PublishMessageResponse {
        return GatewayOuterClass.PublishMessageResponse
            .newBuilder()
            .setKey(key).build()
    }
}