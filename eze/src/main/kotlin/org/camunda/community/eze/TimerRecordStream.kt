/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.protocol.record.value.TimerRecordValue

class TimerRecordStream(private val records: Iterable<Record<TimerRecordValue>>) :
        Iterable<Record<TimerRecordValue>> {

	override fun iterator(): Iterator<Record<TimerRecordValue>> {
        return records.iterator()
    }

	fun withProcessDefinitionKey(processDefinitionKey: Long): TimerRecordStream {
        return TimerRecordStream(filter { it.value.processDefinitionKey == processDefinitionKey })
    }
	
	fun withProcessInstanceKey(processInstanceKey: Long): TimerRecordStream {
        return TimerRecordStream(filter { it.value.processInstanceKey == processInstanceKey })
    }

	fun withTargetElementId(targetElementId: String): TimerRecordStream {
        return TimerRecordStream(filter { it.value.targetElementId == targetElementId })
    }

	fun withElementInstanceKey(elementInstanceKey: Long): TimerRecordStream {
        return TimerRecordStream(filter { it.value.elementInstanceKey == elementInstanceKey })
    }
}