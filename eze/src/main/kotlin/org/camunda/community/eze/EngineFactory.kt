/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.db.ZeebeDb
import io.camunda.zeebe.engine.Engine
import io.camunda.zeebe.engine.api.CommandResponseWriter
import io.camunda.zeebe.engine.api.InterPartitionCommandSender
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributionCommandSender
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.engine.processing.streamprocessor.TypedEventRegistry
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessorContext
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessors
import io.camunda.zeebe.engine.state.ZbColumnFamilies
import io.camunda.zeebe.engine.state.appliers.EventAppliers
import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.log.LogStreamReader
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.protocol.impl.record.CopiedRecord
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.scheduler.Actor
import io.camunda.zeebe.scheduler.ActorScheduler
import io.camunda.zeebe.scheduler.ActorSchedulingService
import io.camunda.zeebe.scheduler.clock.ActorClock
import io.camunda.zeebe.scheduler.clock.ControlledActorClock
import io.camunda.zeebe.streamprocessor.StreamProcessor
import io.camunda.zeebe.streamprocessor.StreamProcessorMode
import io.camunda.zeebe.util.FeatureFlags
import org.camunda.community.eze.db.EzeZeebeDbFactory
import org.camunda.community.eze.grpc.EzeGatewayFactory
import java.nio.file.Files
import java.util.concurrent.CompletableFuture

typealias PartitionId = Int

private const val LOGSTREAM_NAME = "EZE-LOG"

object EngineFactory {

    private val partitionId: PartitionId = 1
    private val partitionCount = 1

    private val streamWritersByPartition = mutableMapOf<PartitionId, LogStreamRecordWriter>()

    private fun streamRecordWriterLookUp(): (Int) -> LogStreamRecordWriter {
        return { partitionId ->
            streamWritersByPartition[partitionId]
                ?: throw RuntimeException("no stream writer found for partition '$partitionId'")
        }
    }

    fun create(exporters: Iterable<Exporter> = emptyList()): ZeebeEngine {

        val clock = createActorClock()

        val scheduler = createActorScheduler(clock)

        val logStorage = createLogStorage()
        val logStream = createLogStream(
            partitionId = partitionId,
            logStorage = logStorage,
            scheduler = scheduler
        )

        val streamWriter = logStream.newLogStreamRecordWriter().join()
        streamWritersByPartition[partitionId] = streamWriter

        val gateway = EzeGatewayFactory.createGateway(
            port = ZeebeEngineImpl.PORT,
            streamWriter = logStream.newLogStreamRecordWriter().join()
        )

        val zeebeDb = createDatabase()

        val partitionCommandSender = createPartitionCommandSender()

        val streamProcessor = createStreamProcessor(
            logStream = logStream,
            database = zeebeDb,
            scheduler = scheduler,
            responseWriter = gateway.getResponseWriter(),
            partitionCommandSender = partitionCommandSender
        )

        val exporterReader = logStream.newLogStreamReader().join()
        val recordStreamReader = logStream.newLogStreamReader().join()

        val exporterRunner = ExporterRunner(
            exporters = exporters,
            reader = { position -> createRecordStream(exporterReader, position) }
        )
        logStream.registerRecordAvailableListener(exporterRunner::onRecordsAvailable)

        return ZeebeEngineImpl(
            startCallback = {
                gateway.start()
                streamProcessor.openAsync(false).join()
                exporterRunner.open()
            },
            stopCallback = {
                gateway.stop()
                exporterRunner.close()
                streamProcessor.close()
                zeebeDb.close()
                logStream.close()
                scheduler.stop()
            },
            recordStream = { createRecordStream(recordStreamReader) },
            clock = clock
        )
    }

    private fun createStreamProcessor(
        logStream: LogStream,
        database: ZeebeDb<ZbColumnFamilies>,
        scheduler: ActorSchedulingService,
        responseWriter: CommandResponseWriter,
        partitionCommandSender: InterPartitionCommandSender
    ): StreamProcessor {
        return StreamProcessor.builder()
            .logStream(logStream)
            .zeebeDb(database)
            .eventApplierFactory { EventAppliers(it) }
            .commandResponseWriter(responseWriter)
            .partitionCommandSender(partitionCommandSender)
            .nodeId(0)
            .actorSchedulingService(scheduler)
            .streamProcessorMode(StreamProcessorMode.PROCESSING)
            .recordProcessors(listOf(createZeebeEngine()))
            .build()
    }

    private fun createPartitionCommandSender(): InterPartitionCommandSender {
        return SinglePartitionCommandSender(
            writerLookUp = streamRecordWriterLookUp()
        )
    }

    private fun createZeebeEngine(): Engine {
        return Engine(createRecordProcessorsFactory())
    }

    private fun createRecordProcessorsFactory(): ((TypedRecordProcessorContext) -> TypedRecordProcessors) {
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

    private fun createDatabase(): ZeebeDb<ZbColumnFamilies> {
        val zeebeDbFactory = EzeZeebeDbFactory.getDefaultFactory<ZbColumnFamilies>()
        return zeebeDbFactory.createDb(Files.createTempDirectory("zeebeDb").toFile())
    }

    private fun createLogStream(
        partitionId: Int,
        logStorage: LogStorage,
        scheduler: ActorSchedulingService
    ): LogStream {
        val builder = LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withLogName(LOGSTREAM_NAME)
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

    private fun createLogStorage(): LogStorage {
        return InMemoryLogStorage()
    }

    private fun createActorScheduler(clock: ActorClock): ActorScheduler {
        val scheduler = ActorScheduler.newActorScheduler()
            .setActorClock(clock)
            .build()

        scheduler.start()

        return scheduler
    }

    private fun createActorClock(): ControlledActorClock {
        return ControlledActorClock()
    }

    private fun createRecordStream(
        reader: LogStreamReader,
        position: Long = -1
    ): Iterable<Record<*>> {

        position.takeIf { it > 0 }
            ?.let { reader.seekToNextEvent(it) }
            ?: reader.seekToFirstEvent()

        val records = mutableListOf<Record<*>>()

        reader.forEach {
            val metadata = RecordMetadata()
            it.readMetadata(metadata)

            val value =
                TypedEventRegistry.EVENT_REGISTRY[metadata.valueType]!!.getDeclaredConstructor()
                    .newInstance()

            it.readValue(value)

            val record = CopiedRecord(
                value,
                metadata,
                it.key,
                partitionId,
                it.position,
                it.sourceEventPosition,
                it.timestamp
            )
            records.add(record)
        }

        return records
    }

}

