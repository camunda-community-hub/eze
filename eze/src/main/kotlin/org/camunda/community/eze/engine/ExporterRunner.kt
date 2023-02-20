/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.exporter.api.context.Configuration
import io.camunda.zeebe.exporter.api.context.Context
import io.camunda.zeebe.exporter.api.context.Controller
import io.camunda.zeebe.exporter.api.context.ScheduledTask
import io.camunda.zeebe.protocol.record.Record
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ExporterRunner(
    exporters: Iterable<Exporter>,
    val reader: (Long) -> Iterable<Record<*>>
) : AutoCloseable {

    private val controller = ExporterController()

    private val exporters: Map<Exporter, ExporterContext> =
        exporters.associateWith { ExporterContext(it) }

    private var lastPosition: Long = -1

    fun open() {
        exporters.forEach { (exporter, context) ->
            exporter.configure(context)
            exporter.open(controller)
        }
    }

    override fun close() {
        exporters.forEach { (exporter, _) -> exporter.close() }
        controller.close()
    }

    fun onRecordsAvailable() {
        reader(lastPosition).forEach { record ->
            exporters
                .filter { (_, context) ->
                    context.recordFilter?.let {
                        it.acceptType(record.recordType) && it.acceptValue(record.valueType)
                    } ?: true
                }
                .forEach { (exporter, context) ->
                    exporter.export(record)
                }
            lastPosition = record.position
        }
    }

    inner class ExporterContext(val exporter: Exporter) : Context {

        var recordFilter: Context.RecordFilter? = null

        override fun getLogger(): Logger {
            return LoggerFactory.getLogger(exporter::class.java)
        }

        override fun getConfiguration(): Configuration {
            return ExporterConfiguration(exporter)
        }

        override fun setFilter(filter: Context.RecordFilter?) {
            recordFilter = filter
        }
    }

    inner class ExporterConfiguration(val exporter: Exporter) : Configuration {
        override fun getId(): String {
            return exporter::class.java.simpleName
        }

        override fun getArguments(): MutableMap<String, Any> {
            return mutableMapOf()
        }

        override fun <T : Any?> instantiate(configClass: Class<T>): T {
            return configClass.getDeclaredConstructor().newInstance()
        }
    }

    inner class ExporterController : Controller, AutoCloseable {

        val executor = Executors.newSingleThreadScheduledExecutor()

        override fun updateLastExportedRecordPosition(position: Long) {
            // currently, not needed
        }

        override fun updateLastExportedRecordPosition(position: Long, metadata: ByteArray?) {
            // currently, not needed
        }

        override fun readMetadata(): Optional<ByteArray> {
            return Optional.empty()
        }

        override fun scheduleCancellableTask(delay: Duration, task: Runnable): ScheduledTask {
            val future =
                executor.scheduleWithFixedDelay(task, 0, delay.toMillis(), TimeUnit.MILLISECONDS)
            return ScheduledTask { future.cancel(true) }
        }

        override fun close() {
            executor.shutdown()
        }
    }
}
