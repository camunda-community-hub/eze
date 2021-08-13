/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db

import org.agrona.ExpandableArrayBuffer
import org.agrona.concurrent.UnsafeBuffer
import io.camunda.zeebe.db.DbKey
import io.camunda.zeebe.db.impl.ZeebeDbConstants
import io.camunda.zeebe.db.DbValue
import org.agrona.DirectBuffer
import java.util.function.ObjIntConsumer
import java.lang.IllegalStateException
import java.util.*

class ColumnFamilyContext internal constructor(private val columnFamilyPrefix: Long) {
    // we can also simply use one buffer
    private val keyBuffer = ExpandableArrayBuffer()
    private val valueBuffer = ExpandableArrayBuffer()
    private val keyViewBuffer: DirectBuffer = UnsafeBuffer(0, 0)
    private val valueViewBuffer: DirectBuffer = UnsafeBuffer(0, 0)
    private val prefixKeyBuffers: Queue<ExpandableArrayBuffer?>
    var keyLength = 0
        private set

    fun writeKey(key: DbKey) {
        keyLength = 0
        keyBuffer.putLong(0, columnFamilyPrefix, ZeebeDbConstants.ZB_DB_BYTE_ORDER)
        keyLength += java.lang.Long.BYTES
        key.write(keyBuffer, java.lang.Long.BYTES)
        keyLength += key.length
    }

    val keyBufferArray: ByteArray
        get() = keyBuffer.byteArray()

    fun writeValue(value: DbValue) {
        value.write(valueBuffer, 0)
    }

    val valueBufferArray: ByteArray
        get() = valueBuffer.byteArray()

    fun wrapKeyView(key: ByteArray?) {
        if (key != null) {
            // wrap without the column family key
            keyViewBuffer.wrap(key, java.lang.Long.BYTES, key.size - java.lang.Long.BYTES)
        } else {
            keyViewBuffer.wrap(ZERO_SIZE_ARRAY)
        }
    }

    val keyView: DirectBuffer?
        get() = if (isKeyViewEmpty) null else keyViewBuffer
    val isKeyViewEmpty: Boolean
        get() = keyViewBuffer.capacity() == ZERO_SIZE_ARRAY.size

    fun wrapValueView(value: ByteArray?) {
        if (value != null) {
            valueViewBuffer.wrap(value)
        } else {
            valueViewBuffer.wrap(ZERO_SIZE_ARRAY)
        }
    }

    val valueView: DirectBuffer?
        get() = if (isValueViewEmpty) null else valueViewBuffer
    val isValueViewEmpty: Boolean
        get() = valueViewBuffer.capacity() == ZERO_SIZE_ARRAY.size

    fun withPrefixKey(key: DbKey, prefixKeyConsumer: ObjIntConsumer<ByteArray?>) {
        checkNotNull(prefixKeyBuffers.peek()) { "Currently nested prefix iterations are not supported! This will cause unexpected behavior." }
        val prefixKeyBuffer = prefixKeyBuffers.remove()
        try {
            prefixKeyBuffer!!.putLong(0, columnFamilyPrefix, ZeebeDbConstants.ZB_DB_BYTE_ORDER)
            key.write(prefixKeyBuffer, java.lang.Long.BYTES)
            val prefixLength = java.lang.Long.BYTES + key.length
            prefixKeyConsumer.accept(prefixKeyBuffer.byteArray(), prefixLength)
        } finally {
            prefixKeyBuffers.add(prefixKeyBuffer)
        }
    }

    companion object {
        private val ZERO_SIZE_ARRAY = ByteArray(0)
    }

    init {
        prefixKeyBuffers = ArrayDeque()
        prefixKeyBuffers.add(ExpandableArrayBuffer())
        prefixKeyBuffers.add(ExpandableArrayBuffer())
    }
}
