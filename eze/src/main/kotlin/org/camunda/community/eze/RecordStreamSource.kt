package org.camunda.community.eze

import io.camunda.zeebe.protocol.record.Record

interface RecordStreamSource {

    fun records(): Iterable<Record<*>>
    
}
