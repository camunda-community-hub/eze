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
import io.camunda.zeebe.protocol.ZbColumnFamilies
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue
import io.camunda.zeebe.protocol.record.RecordType
import io.camunda.zeebe.stream.api.CommandResponseWriter
import io.camunda.zeebe.stream.api.InterPartitionCommandSender
import io.camunda.zeebe.stream.api.ProcessingResult
import io.camunda.zeebe.stream.api.records.TypedRecord
import io.camunda.zeebe.stream.api.scheduling.ProcessingScheduleService
import io.camunda.zeebe.stream.impl.RecordProcessorContextImpl
import io.camunda.zeebe.stream.impl.records.RecordBatchEntry
import io.camunda.zeebe.stream.impl.state.DbKeyGenerator
import io.camunda.zeebe.util.buffer.BufferUtil
import org.camunda.community.eze.records.RecordWrapper
import org.camunda.community.eze.records.RecordsList
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors

class EzeStreamProcessor(
    private val records: RecordsList,
    private val partitionId: Int,
    private val engine: Engine,
    private val responseWriter: CommandResponseWriter,
    private val zeebeDb: ZeebeDb<ZbColumnFamilies>,
    private val interPartitionCommandSender: InterPartitionCommandSender,
    private val processingScheduleService: ProcessingScheduleService,
    private val startCallback: Runnable,
    private val stopCallback: Runnable
) {
    private val logger = LoggerFactory.getLogger("EZE")
    private var lastPosition = 0;
    private val executor = Executors.newSingleThreadExecutor()
    private val transactionContext = zeebeDb.createContext()

    fun start() {
        startCallback.run()
        records.registerAddListener(this::scheduleProcessing)
        scheduleProcessing()

        val transactionContext = transactionContext
        val recordProcessorContextImpl = RecordProcessorContextImpl(
            partitionId,
            processingScheduleService,
            zeebeDb,
            transactionContext,
            interPartitionCommandSender,
            DbKeyGenerator(partitionId, zeebeDb, transactionContext)
        )
        engine.init(recordProcessorContextImpl)
    }

    private fun scheduleProcessing() {
        executor.submit(this::process)
    }

    private fun process() {
//        var currentPosition = lastPosition
        while (lastPosition < records.size) {
            val typedRecord = records[lastPosition]

            if (typedRecord.recordType == RecordType.COMMAND &&
                engine.accepts(typedRecord.valueType)
            ) {
                try {
                    val resultBuilder = BufferedProcessingResultBuilder({ _, _ -> true })
                    transactionContext.runInTransaction {
                        val processingResult = engine.process(typedRecord, resultBuilder)
                        processResult(typedRecord, processingResult)
                    }
                } catch (e: Exception) {
                    try {
                        logger.error("Error on process record {}.", typedRecord, e)
                        val resultBuilder = BufferedProcessingResultBuilder({ _, _ -> true })
                        val onProcessingErrorResult =
                            engine.onProcessingError(e, typedRecord, resultBuilder)
                        processResult(typedRecord, onProcessingErrorResult)
                    } catch (e: Exception) {
                        logger.error("Error on handling processing error. Lets stop that.", e)
                        throw e
                    }
                }
            }
            lastPosition++
        }
//            currentPosition++;
//        }
    }

    private fun processResult(
        typedRecord: TypedRecord<UnifiedRecordValue>,
        processingResult: ProcessingResult
    ) {

        val intermediateList = mutableListOf<TypedRecord<UnifiedRecordValue>>()
        for (record: RecordBatchEntry in processingResult.recordBatch) {
            val typedRecord = RecordWrapper.convert(record)
            intermediateList.add(typedRecord)
        }
        // in order to add a batch of records (atomically) we need to addAll
        records.addAll(intermediateList)

        processingResult.processingResponse.ifPresent {
            val responseValue = it.responseValue()
            val recordMetadata = responseValue.recordMetadata
            val recordValue = responseValue.recordValue()
            responseWriter.recordType(recordMetadata.recordType)
                .intent(recordMetadata.intent)
                .valueType(recordMetadata.valueType)
                .key(responseValue.key)
                .rejectionReason(BufferUtil.wrapString(recordMetadata.rejectionReason))
                .rejectionType(recordMetadata.rejectionType)
                .partitionId(1)
                .valueWriter(recordValue)
                .tryWriteResponse(typedRecord.requestStreamId, typedRecord.requestId)
        }

        processingResult.executePostCommitTasks()
    }

    fun stop() {
        stopCallback.run()
        records.removeAddListener(this::scheduleProcessing)
    }
}
