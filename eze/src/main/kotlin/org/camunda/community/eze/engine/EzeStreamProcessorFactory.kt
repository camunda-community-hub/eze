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
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributionCommandSender
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorContext
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessors
import io.camunda.zeebe.protocol.ZbColumnFamilies
import io.camunda.zeebe.stream.api.CommandResponseWriter
import io.camunda.zeebe.stream.api.InterPartitionCommandSender
import io.camunda.zeebe.stream.api.scheduling.ProcessingScheduleService
import io.camunda.zeebe.util.FeatureFlags
import org.camunda.community.eze.db.EzeZeebeDbFactory
import org.camunda.community.eze.records.RecordsList
import java.nio.file.Files

object EzeStreamProcessorFactory {

    fun createStreamProcessor(
        records: RecordsList,
        responseWriter: CommandResponseWriter,
        partitionId: Int,
    ): EzeStreamProcessor {
        val engine = createZeebeEngine(partitionId)
        val zeebeDb = createDatabase()

        return EzeStreamProcessor(
            records,
            partitionId,
            engine,
            responseWriter,
            zeebeDb,
            createPartitionCommandSender(records),
            createProcessingScheduleService(records),
            startCallback = {
            },
            stopCallback = {
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

    private fun createPartitionCommandSender(records: RecordsList): InterPartitionCommandSender {
        return SinglePartitionCommandSender(
            writer = { record ->
                records.add(record)
            }
        )
    }

    private fun createProcessingScheduleService(records: RecordsList): ProcessingScheduleService {
        return SingleThreadProcessingScheduleService(records)
    }
}
