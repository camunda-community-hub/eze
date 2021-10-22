/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue

class ProcessInstanceRecordStream(private val records: Iterable<Record<ProcessInstanceRecordValue>>) :
        Iterable<Record<ProcessInstanceRecordValue>> {

	override fun iterator(): Iterator<Record<ProcessInstanceRecordValue>> {
        return records.iterator()
    }

	fun withBpmnProcessId(bpmnProcessId: String): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.bpmnProcessId == bpmnProcessId })
    }

	fun withProcessDefinitionKey(processDefinitionKey: Long): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.processDefinitionKey == processDefinitionKey })
    }

	fun withElementId(elementId: String): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.elementId == elementId })
    }

	fun withFlowScopeKey(flowScopeKey: Long): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.flowScopeKey == flowScopeKey })
    }

	fun withParentProcessInstanceKey(parentProcessInstanceKey: Long): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.parentProcessInstanceKey == parentProcessInstanceKey })
    }

	fun withParentElementInstanceKey(parentElementInstanceKey: Long): ProcessInstanceRecordStream {
        return ProcessInstanceRecordStream(filter { it.value.parentElementInstanceKey == parentElementInstanceKey })
    }
}