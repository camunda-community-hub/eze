/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.db.ZeebeDb
import io.camunda.zeebe.engine.Engine
import io.camunda.zeebe.engine.api.CommandResponseWriter
import io.camunda.zeebe.engine.api.InterPartitionCommandSender
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributionCommandSender
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorContext
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessors
import io.camunda.zeebe.engine.state.ZbColumnFamilies
import io.camunda.zeebe.engine.state.appliers.EventAppliers
import io.camunda.zeebe.scheduler.ActorSchedulingService
import io.camunda.zeebe.streamprocessor.StreamProcessor
import io.camunda.zeebe.streamprocessor.StreamProcessorMode
import io.camunda.zeebe.util.FeatureFlags
import org.camunda.community.eze.db.EzeZeebeDbFactory
import java.nio.file.Files

object EzeStreamProcessorFactory {

    fun createStreamProcessor(
        logStream: EzeLogStream,
        responseWriter: CommandResponseWriter,
        scheduler: ActorSchedulingService,
        partitionCount: Int
    ): EzeStreamProcessor {

        val zeebeDb = createDatabase()
        val streamProcessor = StreamProcessor.builder()
            .logStream(logStream.getZeebeLogStream())
            .zeebeDb(zeebeDb)
            .eventApplierFactory { EventAppliers(it) }
            .commandResponseWriter(responseWriter)
            .partitionCommandSender(createPartitionCommandSender(logStream))
            .nodeId(0)
            .actorSchedulingService(scheduler)
            .streamProcessorMode(StreamProcessorMode.PROCESSING)
            .recordProcessors(listOf(createZeebeEngine(partitionCount)))
            .build()

        return EzeStreamProcessor(
            startCallback = {
                streamProcessor.openAsync(false).join()
            },
            stopCallback = {
                streamProcessor.close()
                zeebeDb.close()
            }
        )
    }

    private fun createDatabase(): ZeebeDb<ZbColumnFamilies> {
        val zeebeDbFactory = EzeZeebeDbFactory.getDefaultFactory<ZbColumnFamilies>()
        return zeebeDbFactory.createDb(Files.createTempDirectory("zeebeDb").toFile())
    }

    private fun createZeebeEngine(partitionCount: Int): Engine {
        return Engine(createRecordProcessorsFactory(partitionCount))
    }

    private fun createRecordProcessorsFactory(partitionCount: Int): ((TypedRecordProcessorContext) -> TypedRecordProcessors) {
        return { context ->
            EngineProcessors.createEngineProcessors(
                context,
                partitionCount,
                SubscriptionCommandSender(
                    context.partitionId,
                    context.partitionCommandSender
                ),
                DeploymentDistributionCommandSender(
                    context.partitionId,
                    context.partitionCommandSender
                ),
                { jobType ->
                    // new job is available
                },
                FeatureFlags.createDefault()
            )
        }
    }

    private fun createPartitionCommandSender(logStream: EzeLogStream): InterPartitionCommandSender {

        val streamWriters =
            mapOf(logStream.getZeebeLogStream().partitionId to logStream.createWriter())

        return SinglePartitionCommandSender(
            writerLookUp = { partitionId ->
                streamWriters[partitionId]
                    ?: throw RuntimeException("no stream writer found for partition '$partitionId'")
            }
        )
    }


}
