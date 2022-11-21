/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.records

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.value.ErrorType
import io.camunda.zeebe.protocol.record.value.IncidentRecordValue

class IncidentRecordStream(private val records: Iterable<Record<IncidentRecordValue>>) :
    Iterable<Record<IncidentRecordValue>> {

    override fun iterator(): Iterator<Record<IncidentRecordValue>> {
        return records.iterator()
    }

    fun withErrorType(errorType: ErrorType): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.errorType == errorType })
    }

    fun withBpmnProcessId(bpmnProcessId: String): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.bpmnProcessId == bpmnProcessId })
    }

    fun withProcessDefinitionKey(processDefinitionKey: Long): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.processDefinitionKey == processDefinitionKey })
    }

    fun withProcessInstanceKey(processInstanceKey: Long): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.processInstanceKey == processInstanceKey })
    }

    fun withElementId(elementId: String): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.elementId == elementId })
    }

    fun withElementInstanceKey(elementInstanceKey: Long): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.elementInstanceKey == elementInstanceKey })
    }

    fun withJobKey(jobKey: Long): IncidentRecordStream {
        return IncidentRecordStream(filter { it.value.jobKey == jobKey })
    }

}

