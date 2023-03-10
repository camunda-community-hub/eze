/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.RecordValue
import io.camunda.zeebe.protocol.record.ValueType
import io.camunda.zeebe.protocol.record.value.*
import io.camunda.zeebe.protocol.record.value.deployment.DecisionRecordValue
import io.camunda.zeebe.protocol.record.value.deployment.DecisionRequirementsRecordValue
import io.camunda.zeebe.protocol.record.value.deployment.Process
import org.camunda.community.eze.records.*

interface RecordStreamSource {

    fun records(): Iterable<Record<*>>

    fun <T : RecordValue> Iterable<Record<*>>.ofValueType(valueType: ValueType): Iterable<Record<T>> {
        return filter { it.valueType == valueType }
            .filterIsInstance<Record<T>>()
    }

    fun processInstanceRecords(): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(records().ofValueType(ValueType.PROCESS_INSTANCE))
    }

    fun jobRecords(): JobRecordStream {
        return JobRecordStream(records().ofValueType(ValueType.JOB))
    }

    fun jobBatchRecords(): Iterable<Record<JobBatchRecordValue>> {
        return records().ofValueType(ValueType.JOB_BATCH)
    }

    fun deploymentRecords(): Iterable<Record<DeploymentRecordValue>> {
        return records().ofValueType(ValueType.DEPLOYMENT)
    }

    fun processRecords(): Iterable<Record<Process>> {
        return records().ofValueType(ValueType.PROCESS)
    }

    fun variableRecords(): VariableRecordStream {
        return VariableRecordStream(records().ofValueType(ValueType.VARIABLE))
    }

    fun variableDocumentRecords(): Iterable<Record<VariableDocumentRecordValue>> {
        return records().ofValueType(ValueType.VARIABLE_DOCUMENT)
    }

    fun incidentRecords(): IncidentRecordStream {
        return IncidentRecordStream(records().ofValueType(ValueType.INCIDENT))
    }

    fun timerRecords(): TimerRecordStream {
        return TimerRecordStream(records().ofValueType(ValueType.TIMER))
    }

    fun messageRecords(): Iterable<Record<MessageRecordValue>> {
        return records().ofValueType(ValueType.MESSAGE)
    }

    fun messageSubscriptionRecords(): Iterable<Record<MessageSubscriptionRecordValue>> {
        return records().ofValueType(ValueType.MESSAGE_SUBSCRIPTION)
    }

    fun messageStartEventSubscriptionRecords(): Iterable<Record<MessageStartEventSubscriptionRecordValue>> {
        return records().ofValueType(ValueType.MESSAGE_START_EVENT_SUBSCRIPTION)
    }

    fun processMessageSubscriptionRecords(): Iterable<Record<ProcessMessageSubscriptionRecordValue>> {
        return records().ofValueType(ValueType.PROCESS_MESSAGE_SUBSCRIPTION)
    }

    fun decisionRequirementsRecords(): Iterable<Record<DecisionRequirementsRecordValue>> {
        return records().ofValueType(ValueType.DECISION_REQUIREMENTS)
    }

    fun decisionRecords(): Iterable<Record<DecisionRecordValue>> {
        return records().ofValueType(ValueType.DECISION)
    }

}
