/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.logstreams.log.LogStreamReader
import io.camunda.zeebe.protocol.impl.record.CopiedRecord
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.scheduler.ActorScheduler
import io.camunda.zeebe.scheduler.clock.ActorClock
import io.camunda.zeebe.scheduler.clock.ControlledActorClock
import io.camunda.zeebe.stream.impl.TypedEventRegistry
import org.camunda.community.eze.engine.ExporterRunner
import org.camunda.community.eze.engine.EzeLogStreamFactory
import org.camunda.community.eze.engine.EzeStreamProcessorFactory
import org.camunda.community.eze.engine.ZeebeEngineImpl
import org.camunda.community.eze.grpc.EzeGatewayFactory

typealias PartitionId = Int

object EngineFactory {

    private val partitionId: PartitionId = 1
    private val partitionCount = 1

    fun create(exporters: Iterable<Exporter> = emptyList()): ZeebeEngine {

        val clock = createActorClock()

        val scheduler = createActorScheduler(clock)

        val logStream = EzeLogStreamFactory.createLogStream(
            partitionId = partitionId,
            scheduler = scheduler
        )

        val gateway = EzeGatewayFactory.createGateway(
            port = ZeebeEngineImpl.PORT,
            streamWriter = logStream.createWriter()
        )

        val streamProcessor = EzeStreamProcessorFactory.createStreamProcessor(
            logStream = logStream,
            responseWriter = gateway.getResponseWriter(),
            scheduler = scheduler,
            partitionCount = partitionCount
        )

        val exporterReader = logStream.createReader()
        val recordStreamReader = logStream.createReader()

        val exporterRunner = ExporterRunner(
            exporters = exporters,
            reader = { position -> createRecordStream(exporterReader, position) }
        )
        logStream.registerRecordAvailableListener(exporterRunner::onRecordsAvailable)

        return ZeebeEngineImpl(
            startCallback = {
                gateway.start()
                streamProcessor.start()
                exporterRunner.open()
            },
            stopCallback = {
                gateway.stop()
                exporterRunner.close()
                streamProcessor.stop()
                logStream.close()
                scheduler.stop()
            },
            recordStream = { createRecordStream(recordStreamReader) },
            clock = clock
        )
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

