/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.protocol.record.RecordValue
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.value.*
import io.camunda.zeebe.protocol.record.value.deployment.Process
import io.camunda.zeebe.test.util.record.CompactRecordLogger

object RecordStream {

    fun Iterable<Record<*>>.print(compact: Boolean = true) {
        if (compact) {
            CompactRecordLogger(toList()).log()
        } else {
            println("===== records (count: ${count()}) =====")
            toList().forEach { println(it.toJson()) }
            println("---------------------------")
        }
    }

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

    fun Iterable<Record<*>>.job(): Iterable<Record<JobRecordValue>> {
        return ofValueType(ValueType.JOB)
    }

    fun Iterable<Record<*>>.jobBatch(): Iterable<Record<JobBatchRecordValue>> {
        return ofValueType(ValueType.JOB_BATCH)
    }

    fun Iterable<Record<*>>.deployment(): Iterable<Record<DeploymentRecordValue>> {
        return ofValueType(ValueType.DEPLOYMENT)
    }

    fun Iterable<Record<*>>.process(): Iterable<Record<Process>> {
        return ofValueType(ValueType.PROCESS)
    }

    fun Iterable<Record<*>>.variable(): Iterable<Record<VariableRecordValue>> {
        return ofValueType(ValueType.VARIABLE)
    }

    fun Iterable<Record<*>>.variableDocument(): Iterable<Record<VariableDocumentRecordValue>> {
        return ofValueType(ValueType.VARIABLE_DOCUMENT)
    }

    fun Iterable<Record<*>>.incident(): Iterable<Record<IncidentRecordValue>> {
        return ofValueType(ValueType.INCIDENT)
    }

    fun Iterable<Record<*>>.timer(): Iterable<Record<TimerRecordValue>> {
        return ofValueType(ValueType.TIMER)
    }

    fun Iterable<Record<*>>.message(): Iterable<Record<MessageRecordValue>> {
        return ofValueType(ValueType.MESSAGE)
    }

    fun Iterable<Record<*>>.messageSubscription(): Iterable<Record<MessageSubscriptionRecordValue>> {
        return ofValueType(ValueType.MESSAGE_SUBSCRIPTION)
    }

    fun Iterable<Record<*>>.messageStartEventSubscription(): Iterable<Record<MessageStartEventSubscriptionRecordValue>> {
        return ofValueType(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION)
    }

    fun Iterable<Record<*>>.processMessageSubscription(): Iterable<Record<ProcessMessageSubscriptionRecordValue>> {
        return ofValueType(ValueType.PROCESS_MESSAGE_SUBSCRIPTION)
    }

    fun Iterable<Record<ProcessInstanceRecordValue>>.ofElementType(elementType: BpmnElementType): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.bpmnElementType == elementType }
    }

    fun Iterable<Record<ProcessInstanceRecordValue>>.ofProcessInstance(processInstanceKey: Long): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.processInstanceKey == processInstanceKey }
    }


    fun Iterable<Record<JobRecordValue>>.ofJobType(jobType: String): Iterable<Record<JobRecordValue>> {
        return filter { it.value.type == jobType }
    }

}
