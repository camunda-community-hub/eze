/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.scheduler.ActorSchedulingService
import io.camunda.zeebe.scheduler.clock.ActorClock
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

private const val LOG_STREAM_NAME = "EZE-LOG"

object EzeLogStreamFactory {

    fun createLogStream(
        partitionId: Int,
        scheduler: ActorSchedulingService,
        clock: ActorClock
    ): EzeLogStream {

        val logStorage = createLogStorage()
        val logStream = createLogStream(
            partitionId = partitionId,
            logStorage = logStorage,
            scheduler = scheduler,
            clock = clock
        )

        return EzeLogStream(
            logStream = logStream
        )
    }

    private fun createLogStorage(): LogStorage {
        return InMemoryLogStorage()
    }

    private fun createLogStream(
        partitionId: Int,
        logStorage: LogStorage,
        scheduler: ActorSchedulingService,
        clock: ActorClock
    ): LogStream {
        val builder = LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withLogName(LOG_STREAM_NAME)
            .withClock(clock)
            .withMeterRegistry(SimpleMeterRegistry())

        return builder.build()
    }

}
