/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.protocol.impl.record.CopiedRecord
import io.camunda.zeebe.protocol.impl.record.RecordMetadata
import io.camunda.zeebe.protocol.impl.record.VersionInfo
import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.scheduler.ActorScheduler
import io.camunda.zeebe.scheduler.clock.ActorClock
import io.camunda.zeebe.scheduler.clock.ControlledActorClock
import org.camunda.community.eze.engine.EzeStreamProcessorFactory
import org.camunda.community.eze.engine.ZeebeEngineImpl
import org.camunda.community.eze.grpc.EzeGatewayFactory
import org.camunda.community.eze.records.RecordsList
import java.util.concurrent.CopyOnWriteArrayList

typealias PartitionId = Int

object EngineFactory {

    private val partitionId: PartitionId = 1
    private val partitionCount = 1


    fun create(exporters: Iterable<Exporter> = emptyList()): ZeebeEngine {

        val clock = createActorClock()

        val records = RecordsList(CopyOnWriteArrayList())
        val gateway = EzeGatewayFactory.createGateway(
            port = ZeebeEngineImpl.PORT,
            records = records
        )

        val streamProcessor = EzeStreamProcessorFactory.createStreamProcessor(
            records = records,
            responseWriter = gateway.getResponseWriter(),
            partitionCount = partitionCount
        )
//
//        val exporterReader = logStream.createReader()
//        val recordStreamReader = logStream.createReader()
//
//        val exporterRunner = ExporterRunner(
//            exporters = exporters,
//            reader = { position -> createRecordStream(exporterReader, position) }
//        )
//        logStream.registerRecordAvailableListener(exporterRunner::onRecordsAvailable)

        return ZeebeEngineImpl(
            startCallback = {
                gateway.start()
                streamProcessor.start()
//                exporterRunner.open()
            },
            stopCallback = {
                gateway.stop()
//                exporterRunner.close()
                streamProcessor.stop()
            },
            recordStream = { createRecordStream(records) },
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
        recordsList: RecordsList,
        position: Long = -1
    ): Iterable<Record<*>> {

        val records = mutableListOf<Record<*>>()

        recordsList.forEach {
            val metadata = RecordMetadata()
            metadata.valueType(it.valueType)
                .intent(it.intent)
                .recordType(it.recordType)
                .brokerVersion(VersionInfo.parse(it.brokerVersion))
                .rejectionReason(it.rejectionReason)
                .rejectionType(it.rejectionType)
                .requestId(it.requestId)
                .requestStreamId(it.requestStreamId)

            val record = CopiedRecord(
                it.value,
                metadata,
                it.key,
                partitionId,
                it.position,
                it.sourceRecordPosition,
                it.timestamp
            )
            records.add(record)
        }

        return records
    }

}

