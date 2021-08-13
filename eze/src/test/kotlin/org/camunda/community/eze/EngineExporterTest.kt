/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.exporter.api.Exporter
import io.camunda.zeebe.model.bpmn.Bpmn
import io.camunda.zeebe.protocol.record.Record
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(PrintRecordStreamExtension::class)
class EngineExporterTest {

    private lateinit var zeebeEngine: ZeebeEngine

    private val testExporter = TestExporter()

    inner class TestExporter : Exporter {

        val records = mutableListOf<Record<*>>()

        override fun export(record: Record<*>) {
            records.add(record)
        }
    }

    @BeforeEach
    fun `setup grpc server`() {
        zeebeEngine = EngineFactory.create(exporters = listOf(testExporter))
        zeebeEngine.start()

        PrintRecordStreamExtension.zeebeEngine = zeebeEngine
    }

    @AfterEach
    fun `tear down`() {
        zeebeEngine.stop()
        testExporter.records.clear()
    }

    @Test
    fun `should export records`() {
        // given
        val zeebeClient = zeebeEngine.createClient()

        // when
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()

        zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
        await.untilAsserted {
            assertThat(testExporter.records)
                .isNotEmpty
                .hasSameSizeAs(zeebeEngine.records())

            assertThat(testExporter.records.map { it.toJson() })
                .isEqualTo(zeebeEngine.records().map { it.toJson() })
        }
    }

}
