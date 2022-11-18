/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.records

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.value.VariableRecordValue

class VariableRecordStream(private val records: Iterable<Record<VariableRecordValue>>) :
    Iterable<Record<VariableRecordValue>> {

    override fun iterator(): Iterator<Record<VariableRecordValue>> {
        return records.iterator()
    }

    fun withVariableName(name: String): VariableRecordStream {
        return VariableRecordStream(filter { it.value.name == name })
    }

    fun withVariableValue(value: String): VariableRecordStream {
        return VariableRecordStream(filter { it.value.value == value })
    }

    fun withScopeKey(scopeKey: Long): VariableRecordStream {
        return VariableRecordStream(filter { it.value.scopeKey == scopeKey })
    }

    fun withProcessInstanceKey(processInstanceKey: Long): VariableRecordStream {
        return VariableRecordStream(filter { it.value.processInstanceKey == processInstanceKey })
    }

    fun withProcessDefinitionKey(processDefinitionKey: Long): VariableRecordStream {
        return VariableRecordStream(filter { it.value.processDefinitionKey == processDefinitionKey })
    }
}
