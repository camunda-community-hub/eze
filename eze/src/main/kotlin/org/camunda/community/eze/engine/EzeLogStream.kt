/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.log.LogRecordAwaiter
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.log.LogStreamReader
import io.camunda.zeebe.logstreams.log.LogStreamWriter

class EzeLogStream(private val logStream: LogStream) {

    fun createWriter(): LogStreamWriter {
        return logStream.newLogStreamWriter().join()
    }

    fun createReader(): LogStreamReader {
        return logStream.newLogStreamReader().join()
    }

    fun getZeebeLogStream(): LogStream = logStream

    fun registerRecordAvailableListener(listener: LogRecordAwaiter) {
        logStream.registerRecordAvailableListener(listener)
    }

    fun close() {
        logStream.close()
    }

}
