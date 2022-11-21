/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.records

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.value.JobRecordValue


class JobRecordStream(private val records: Iterable<Record<JobRecordValue>>) :
    Iterable<Record<JobRecordValue>> {

    override fun iterator(): Iterator<Record<JobRecordValue>> {
        return records.iterator()
    }

    fun withJobType(jobType: String): JobRecordStream {
        return JobRecordStream(filter { it.value.type == jobType })
    }

    fun withBpmnProcessId(bpmnProcessId: String): JobRecordStream {
        return JobRecordStream(filter { it.value.bpmnProcessId == bpmnProcessId })
    }

    fun withProcessDefinitionKey(processDefinitionKey: Long): JobRecordStream {
        return JobRecordStream(filter { it.value.processDefinitionKey == processDefinitionKey })
    }

    fun withProcessDefinitionVersion(processDefinitionVersion: Int): JobRecordStream {
        return JobRecordStream(filter { it.value.processDefinitionVersion == processDefinitionVersion })
    }

    fun withElementId(elementId: String): JobRecordStream {
        return JobRecordStream(filter { it.value.elementId == elementId })
    }

    fun withElementInstanceKey(elementInstanceKey: Long): JobRecordStream {
        return JobRecordStream(filter { it.value.elementInstanceKey == elementInstanceKey })
    }

    fun withJobWorker(jobWorker: String): JobRecordStream {
        return JobRecordStream(filter { it.value.worker == jobWorker })
    }

    fun withProcessInstanceKey(processInstanceKey: Long): JobRecordStream {
        return JobRecordStream(filter { it.value.processInstanceKey == processInstanceKey })
    }
}
