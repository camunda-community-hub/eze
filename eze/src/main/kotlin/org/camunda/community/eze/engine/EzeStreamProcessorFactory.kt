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
import io.camunda.zeebe.engine.EngineConfiguration
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.engine.processing.streamprocessor.JobStreamer
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorContext
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessors
import io.camunda.zeebe.protocol.ZbColumnFamilies
import io.camunda.zeebe.scheduler.ActorSchedulingService
import io.camunda.zeebe.scheduler.clock.ActorClock
import io.camunda.zeebe.stream.api.CommandResponseWriter
import io.camunda.zeebe.stream.api.InterPartitionCommandSender
import io.camunda.zeebe.stream.api.StreamClock
import io.camunda.zeebe.stream.impl.StreamProcessor
import io.camunda.zeebe.stream.impl.StreamProcessorMode
import io.camunda.zeebe.util.FeatureFlags
import io.camunda.search.clients.SearchClientsProxy
import io.camunda.security.auth.BrokerRequestAuthorizationConverter
import io.camunda.security.configuration.SecurityConfiguration
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.camunda.community.eze.db.EzeZeebeDbFactory
import java.nio.file.Files
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private const val STREAM_PROCESSOR_START_TIMEOUT_SECONDS = 30L

object EzeStreamProcessorFactory {

    fun createStreamProcessor(
        logStream: EzeLogStream,
        responseWriter: CommandResponseWriter,
        scheduler: ActorSchedulingService,
        partitionCount: Int,
        clock: ActorClock
    ): EzeStreamProcessor {

        val zeebeDb = createDatabase()
        val streamProcessor = StreamProcessor.builder()
            .logStream(logStream.getZeebeLogStream())
            .zeebeDb(zeebeDb)
            .commandResponseWriter(responseWriter)
            .partitionCommandSender(createPartitionCommandSender(logStream))
            .nodeId(0)
            .actorSchedulingService(scheduler)
            .streamProcessorMode(StreamProcessorMode.PROCESSING)
            .meterRegistry(SimpleMeterRegistry())
            .clock(StreamClock.controllable(clock))
            .recordProcessors(listOf(createZeebeEngine(partitionCount)))
            .build()

        return EzeStreamProcessor(
            startCallback = {
                openStreamProcessor(streamProcessor)
            },
            stopCallback = {
                streamProcessor.close()
                zeebeDb.close()
            }
        )
    }

    private fun openStreamProcessor(streamProcessor: StreamProcessor) {
        try {
            streamProcessor.openAsync(false)
                .get(STREAM_PROCESSOR_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (e: TimeoutException) {
            closeStreamProcessorAsync(streamProcessor)
            throw IllegalStateException(
                "Timed out after $STREAM_PROCESSOR_START_TIMEOUT_SECONDS seconds while starting " +
                    "the Zeebe stream processor. Health report: ${streamProcessor.getHealthReport()}",
                e
            )
        } catch (e: ExecutionException) {
            closeStreamProcessorAsync(streamProcessor)
            throw IllegalStateException(
                "Failed to start the Zeebe stream processor. Health report: " +
                    streamProcessor.getHealthReport(),
                e.cause ?: e
            )
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            closeStreamProcessorAsync(streamProcessor)
            throw IllegalStateException(
                "Interrupted while starting the Zeebe stream processor. Health report: " +
                    streamProcessor.getHealthReport(),
                e
            )
        }
    }

    private fun closeStreamProcessorAsync(streamProcessor: StreamProcessor) {
        try {
            streamProcessor.closeAsync()
        } catch (_: RuntimeException) {
            // Startup is already failing; do not hide the original error with cleanup failures.
        }
    }

    private fun createDatabase(): ZeebeDb<ZbColumnFamilies> {
        val zeebeDbFactory = EzeZeebeDbFactory.getDefaultFactory<ZbColumnFamilies>()
        return zeebeDbFactory.createDb(Files.createTempDirectory("zeebeDb").toFile())
    }

    private fun createZeebeEngine(partitionCount: Int): Engine {
        val securityConfig = SecurityConfiguration()
        securityConfig.getAuthorizations().setEnabled(false)
        val engineConfiguration = EngineConfiguration()
            .setEnableAuthorization(false)
            .setEnableIdentitySetup(false)
            .setJobMetricsExportEnabled(false)

        return Engine(
            createRecordProcessorsFactory(partitionCount, securityConfig),
            engineConfiguration,
            securityConfig
        )
    }

    private fun createRecordProcessorsFactory(
        partitionCount: Int,
        securityConfig: SecurityConfiguration
    ): ((TypedRecordProcessorContext) -> TypedRecordProcessors) {
        return { context ->
            EngineProcessors.createEngineProcessors(
                context,
                partitionCount,
                SubscriptionCommandSender(
                    context.partitionId,
                    context.partitionCommandSender
                ),
                context.partitionCommandSender,
                FeatureFlags.createDefault(),
                JobStreamer.noop(),
                SearchClientsProxy.noop(),
                BrokerRequestAuthorizationConverter(securityConfig)
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
