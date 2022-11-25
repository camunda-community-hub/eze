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
import io.camunda.zeebe.scheduler.Actor
import io.camunda.zeebe.scheduler.ActorSchedulingService
import java.util.concurrent.CompletableFuture

private const val LOG_STREAM_NAME = "EZE-LOG"

object EzeLogStreamFactory {

    fun createLogStream(
        partitionId: Int,
        scheduler: ActorSchedulingService
    ): EzeLogStream {

        val logStorage = createLogStorage()
        val logStream = createLogStream(
            partitionId = partitionId,
            logStorage = logStorage,
            scheduler = scheduler
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
        scheduler: ActorSchedulingService
    ): LogStream {
        val builder = LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withLogName(LOG_STREAM_NAME)
            .withActorSchedulingService(scheduler)

        val theFuture = CompletableFuture<LogStream>()

        scheduler.submitActor(Actor.wrap {
            builder
                .buildAsync()
                .onComplete { logStream, failure ->
                    if (failure != null) {
                        theFuture.completeExceptionally(failure)
                    } else {
                        theFuture.complete(logStream)
                    }
                }
        })

        return theFuture.join()
    }

}
