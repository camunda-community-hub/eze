/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.db.ZeebeDb
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor
import io.camunda.zeebe.engine.processing.streamprocessor.TypedEventRegistry
import io.camunda.zeebe.engine.state.ZbColumnFamilies
import io.camunda.zeebe.engine.state.appliers.EventAppliers
import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.log.LogStreamReader
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.protocol.impl.record.CopiedRecord
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.util.sched.Actor
import io.camunda.zeebe.util.sched.ActorScheduler
import io.camunda.zeebe.util.sched.ActorSchedulingService
import io.camunda.zeebe.util.sched.clock.ActorClock
import io.camunda.zeebe.util.sched.clock.ControlledActorClock
import io.grpc.ServerBuilder
import org.camunda.community.eze.db.EzeZeebeDbFactory
import java.nio.file.Files
import java.util.concurrent.CompletableFuture

object EngineFactory {

    private val subscriptionCommandSenderFactory = SubscriptionCommandSenderFactory { partitionId ->
        TODO("return record writer")
    }

    fun create(exporters: Iterable<Exporter> = emptyList()): ZeebeEngine {

        val clock = createActorClock()

        val scheduler = createActorScheduler(clock)

        val logStorage = createLogStorage()
        val logStream = createLogStream(
            partitionId = 1,
            logStorage = logStorage,
            scheduler = scheduler
        )

        val streamWriter = logStream.newLogStreamRecordWriter().join()
        val simpleGateway = SimpleGateway(streamWriter)
        val server = ServerBuilder.forPort(ZeebeEngineImpl.PORT).addService(simpleGateway).build()

        val db = createDatabase()

        val grpcResponseWriter = GrpcResponseWriter(
            responseCallback = simpleGateway::responseCallback,
            errorCallback = simpleGateway::errorCallback
        )

        val streamProcessor = createStreamProcessor(
            partitionCount = 1,
            logStream = logStream,
            database = db,
            scheduler = scheduler,
            grpcResponseWriter
        )

        val reader = logStream.newLogStreamReader().join()

        val exporterRunner = ExporterRunner(
            exporters = exporters,
            reader = { position -> createRecordStream(reader, position) }
        )
        logStream.registerRecordAvailableListener(exporterRunner::onRecordsAvailable)

        return ZeebeEngineImpl(
            startCallback = {
                server.start()
                streamProcessor.openAsync(false).join()
                exporterRunner.open()
            },
            stopCallback = {
                server.shutdownNow()
                server.awaitTermination()
                simpleGateway.close()
                exporterRunner.close()
                streamProcessor.close()
                db.close()
                logStream.close()
                scheduler.stop()
            },
            recordStream = { createRecordStream(reader) },
            clock = clock
        )
    }

    private fun createStreamProcessor(
        partitionCount: Int,
        logStream: LogStream,
        database: ZeebeDb<ZbColumnFamilies>,
        scheduler: ActorSchedulingService,
        grpcResponseWriter: GrpcResponseWriter
    ): StreamProcessor {
        return StreamProcessor.builder()
            .logStream(logStream)
            .zeebeDb(database)
            .eventApplierFactory { EventAppliers(it) }
            .commandResponseWriter(grpcResponseWriter)
            .streamProcessorFactory { context ->
                EngineProcessors.createEngineProcessors(
                    context,
                    partitionCount,
                    subscriptionCommandSenderFactory.ofPartition(partitionId = 1),
                    SinglePartitionDeploymentDistributor(),
                    SinglePartitionDeploymentResponder(),
                    { jobType ->
                        // new job is available
                    }
                )
            }
            .actorSchedulingService(scheduler)
            .build()
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
            .withActorSchedulingService(scheduler)

        val theFuture = CompletableFuture<LogStream>()

        scheduler.submitActor(Actor.wrap {
            builder
                .buildAsync()
                .onComplete { logStream, failure ->
                    // TODO boom
                    theFuture.complete(logStream)
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
                1,
                it.position,
                it.sourceEventPosition,
                it.timestamp
            )
            records.add(record)
        }

        return records
    }

}
