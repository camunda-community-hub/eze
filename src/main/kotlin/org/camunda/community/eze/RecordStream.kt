package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RecordValue
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.value.BpmnElementType
import io.camunda.zeebe.protocol.record.value.JobRecordValue
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue

object RecordStream {

    fun <T : RecordValue> Iterable<Record<T>>.ofRecordType(
        commands: Boolean = false,
        events: Boolean = false,
        rejections: Boolean = false
    ): Iterable<Record<T>> {
        return filter {
            commands && it.recordType == RecordType.COMMAND ||
                    events && it.recordType == RecordType.EVENT ||
                    rejections && it.recordType == RecordType.COMMAND_REJECTION
        }
    }

    fun <T : RecordValue> Iterable<Record<T>>.events(): Iterable<Record<T>> {
        return ofRecordType(events = true)
    }

    fun <T : RecordValue> Iterable<Record<T>>.intent(intent: Intent): Iterable<Record<T>> {
        return filter { it.intent == intent }
    }

    fun <T : RecordValue> Iterable<Record<T>>.key(key: Long): Iterable<Record<T>> {
        return filter { it.key == key }
    }

    fun <T : RecordValue> Iterable<Record<*>>.ofValueType(valueType: ValueType): Iterable<Record<T>> {
        return filter { it.valueType == valueType }
            .filterIsInstance<Record<T>>()
    }

    fun Iterable<Record<*>>.processInstance(): Iterable<Record<ProcessInstanceRecordValue>> {
        return ofValueType(ValueType.PROCESS_INSTANCE)
    }

    fun Iterable<Record<ProcessInstanceRecordValue>>.ofElementType(elementType: BpmnElementType): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.bpmnElementType == elementType }
    }

    fun Iterable<Record<ProcessInstanceRecordValue>>.ofProcessInstance(processInstanceKey: Long): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.processInstanceKey == processInstanceKey }
    }

    fun Iterable<Record<*>>.job(): Iterable<Record<JobRecordValue>> {
        return ofValueType(ValueType.JOB)
    }

    fun Iterable<Record<JobRecordValue>>.ofJobType(jobType: String): Iterable<Record<JobRecordValue>> {
        return filter { it.value.type == jobType }
    }

}
