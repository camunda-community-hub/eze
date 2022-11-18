package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.log.LogRecordAwaiter
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.log.LogStreamReader
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter

class EzeLogStream(private val logStream: LogStream) {

    fun createWriter(): LogStreamRecordWriter {
        return logStream.newLogStreamRecordWriter().join()
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
