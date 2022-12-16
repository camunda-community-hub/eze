/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.records

import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.stream.api.records.TypedRecord

class RecordsList(private val backedList: MutableList<TypedRecord<UnifiedRecordValue>>) : MutableList<TypedRecord<UnifiedRecordValue>> by backedList {

    private val listeners = mutableListOf<Runnable>()

    fun registerAddListener(listener: Runnable) {
        listeners.add(listener)
    }

    fun removeAddListener(listener: Runnable) {
        listeners.remove(listener)
    }

    override fun add(element: TypedRecord<UnifiedRecordValue>): Boolean {
        val result = backedList.add(element)
        listeners.forEach { it.run() }
        return result
    }

    override fun add(index: Int, element: TypedRecord<UnifiedRecordValue>) {
        backedList.add(index, element)
        listeners.forEach { it.run() }
    }

    override fun addAll(
        index: Int,
        elements: Collection<TypedRecord<UnifiedRecordValue>>
    ): Boolean {
        val result = backedList.addAll(index, elements)
        listeners.forEach { it.run() }
        return result
    }

    override fun addAll(elements: Collection<TypedRecord<UnifiedRecordValue>>): Boolean {
        val result = backedList.addAll(elements)
        listeners.forEach { it.run() }
        return result
    }

    override fun clear() {
        throw UnsupportedOperationException()
    }

    override fun remove(element: TypedRecord<UnifiedRecordValue>): Boolean {
        throw UnsupportedOperationException()
    }

    override fun removeAll(elements: Collection<TypedRecord<UnifiedRecordValue>>): Boolean {
        throw UnsupportedOperationException()
    }

    override fun removeAt(index: Int): TypedRecord<UnifiedRecordValue> {
        throw UnsupportedOperationException()
    }

    override fun retainAll(elements: Collection<TypedRecord<UnifiedRecordValue>>): Boolean {
        throw UnsupportedOperationException()
    }
}
