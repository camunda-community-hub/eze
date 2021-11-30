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
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.value.BpmnElementType
import io.camunda.zeebe.protocol.record.value.JobRecordValue
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue
import io.camunda.zeebe.test.util.record.CompactRecordLogger

object RecordStream {

    fun Iterable<Record<*>>.print(compact: Boolean = true) {
        val count = count()
        if (count < 1) {
            println("No records to print.")
        }

        if (compact) {
            CompactRecordLogger(toList()).log()
        } else {
            println("===== records (count: $count) =====")
            toList().forEach { println(it.toJson()) }
            println("---------------------------")
        }
    }

    fun <T : RecordValue> Iterable<Record<T>>.withRecordType(
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
        return withRecordType(events = true)
    }

    fun <T : RecordValue> Iterable<Record<T>>.withIntent(intent: Intent): Iterable<Record<T>> {
        return filter { it.intent == intent }
    }

    fun <T : RecordValue> Iterable<Record<T>>.withKey(key: Long): Iterable<Record<T>> {
        return filter { it.key == key }
    }


    fun Iterable<Record<ProcessInstanceRecordValue>>.withElementType(elementType: BpmnElementType): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.bpmnElementType == elementType }
    }

    fun Iterable<Record<ProcessInstanceRecordValue>>.withProcessInstanceKey(processInstanceKey: Long): Iterable<Record<ProcessInstanceRecordValue>> {
        return filter { it.value.processInstanceKey == processInstanceKey }
    }
}
