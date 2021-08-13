/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.client.api.command.ClientException
import io.camunda.zeebe.client.api.response.ActivatedJob
import io.camunda.zeebe.client.api.response.PartitionBrokerHealth
import io.camunda.zeebe.client.api.response.PartitionBrokerRole
import io.camunda.zeebe.model.bpmn.Bpmn
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.intent.JobIntent
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent
import io.camunda.zeebe.protocol.record.intent.TimerIntent
import io.camunda.zeebe.protocol.record.value.BpmnElementType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.await
import org.camunda.community.eze.RecordStream.print
import org.camunda.community.eze.RecordStream.withElementType
import org.camunda.community.eze.RecordStream.withIntent
import org.camunda.community.eze.RecordStream.withKey
import org.camunda.community.eze.RecordStream.withRecordType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration


class EngineClientTest {

    private lateinit var zeebeEngine: ZeebeEngine

    @BeforeEach
    fun `setup grpc server`() {
        zeebeEngine = EngineFactory.create()
        zeebeEngine.start()
    }

    @AfterEach
    fun `tear down`() {
        zeebeEngine.stop()
    }

    @Test
    fun `should request topology`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val topology = zeebeClient
            .newTopologyRequest()
            .send()
            .join()

        // then
        assertThat(topology.clusterSize).isEqualTo(1)
        assertThat(topology.replicationFactor).isEqualTo(1)
        assertThat(topology.partitionsCount).isEqualTo(1)
        assertThat(topology.gatewayVersion).isEqualTo("A.B.C")

        assertThat(topology.brokers).hasSize(1)
        val broker = topology.brokers[0]
        assertThat(broker.host).isEqualTo("0.0.0.0")
        assertThat(broker.port).isEqualTo(26500)
        assertThat(broker.version).isEqualTo("X.Y.Z")

        assertThat(broker.partitions).hasSize(1)
        val partition = broker.partitions[0]
        assertThat(partition.health).isEqualTo(PartitionBrokerHealth.HEALTHY)
        assertThat(partition.isLeader).isTrue()
        assertThat(partition.role).isEqualTo(PartitionBrokerRole.LEADER)
        assertThat(partition.partitionId).isEqualTo(1)
    }

    @Test
    fun `should use built in client`() {
        // given
        val zeebeClient = zeebeEngine.createClient()

        // when
        val topology = zeebeClient
            .newTopologyRequest()
            .send()
            .join()

        // then
        assertThat(topology).isNotNull
    }

    @Test
    fun `should use gateway address to build client`() {
        // given
        val zeebeClient = ZeebeClient
            .newClientBuilder()
            .usePlaintext()
            .gatewayAddress(zeebeEngine.getGatewayAddress())
            .build();

        // when
        val topology = zeebeClient
            .newTopologyRequest()
            .send()
            .join()

        // then
        assertThat(topology).isNotNull
    }


    @Test
    fun `should publish message`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val message = zeebeClient
            .newPublishMessageCommand()
            .messageName("msg")
            .correlationKey("var")
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
        assertThat(message.messageKey).isPositive
    }

    @Test
    fun `should deploy process`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val deployment = zeebeClient
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

        // then
        assertThat(deployment.key).isPositive
        assertThat(deployment.processes).isNotEmpty

        val process = deployment.processes[0]

        assertThat(process.version).isEqualTo(1)
        assertThat(process.resourceName).isEqualTo("simpleProcess.bpmn")
        assertThat(process.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(process.processDefinitionKey).isPositive
    }

    @Test
    fun `should create instance without variables`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
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

        // when
        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .send()
            .join()

        // then
        assertThat(processInstance.processInstanceKey).isPositive
        assertThat(processInstance.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstance.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstance.version).isEqualTo(1)
    }

    @Test
    fun `should reject command`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val future = zeebeClient.newCreateInstanceCommand()
            .bpmnProcessId("process")
            .latestVersion()
            .send()

        // then
        assertThatThrownBy { future.join() }
            .isInstanceOf(ClientException::class.java)
            .hasMessage("Command 'CREATE' rejected with code 'NOT_FOUND': Expected to find process definition with process ID 'process', but none found")
    }

    @Test
    fun `should create process instance`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
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

        // when
        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
        assertThat(processInstance.processInstanceKey).isPositive
        assertThat(processInstance.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstance.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstance.version).isEqualTo(1)
    }

    @Test
    fun `should cancel process instance`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()

        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // when - then
        zeebeClient
            .newCancelInstanceCommand(processInstance.processInstanceKey)
            .send()
            .join()

        await.untilAsserted {
            val processTerminated = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_TERMINATED)
                .withElementType(BpmnElementType.PROCESS)
                .firstOrNull()

            assertThat(processTerminated).isNotNull
        }
    }

    @Test
    fun `should update variables on process instance`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()

        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // when
        val variablesResponse = zeebeClient
            .newSetVariablesCommand(processInstance.processInstanceKey)
            .variables(mapOf("test123" to 234))
            .local(true)
            .send()
            .join()

        // then
        assertThat(variablesResponse).isNotNull
        assertThat(variablesResponse.key).isPositive
    }

    @Test
    fun `should create process instance with result`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        val deployment = zeebeClient
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

        // when
        val processInstanceResult =
            zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
                .latestVersion()
                .variables(mapOf("test" to 1))
                .withResult()
                .send()
                .join()

        // then
        assertThat(processInstanceResult.processInstanceKey).isPositive
        assertThat(processInstanceResult.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstanceResult.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstanceResult.version).isEqualTo(1)
        assertThat(processInstanceResult.variablesAsMap).containsEntry("test", 1)
    }

    @Test
    fun `should activate job`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()


        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        await.untilAsserted {
            // when
            val activateJobsResponse = zeebeClient
                .newActivateJobsCommand()
                .jobType("jobType")
                .maxJobsToActivate(32)
                .timeout(Duration.ofMinutes(1))
                .workerName("yolo")
                .fetchVariables(listOf("test"))
                .send()
                .join()

            // then
            val jobs = activateJobsResponse.jobs
            assertThat(jobs).isNotEmpty
            assertThat(jobs[0].bpmnProcessId).isEqualTo("simpleProcess")
            assertThat(jobs[0].processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
            assertThat(jobs[0].processInstanceKey).isEqualTo(processInstance.processInstanceKey)
            assertThat(jobs[0].retries).isEqualTo(3)
            assertThat(jobs[0].type).isEqualTo("jobType")
            assertThat(jobs[0].worker).isEqualTo("yolo")
        }
    }

    @Test
    fun `should complete job`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("process")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("test") }
                    .endEvent()
                    .done(),
                "process.bpmn"
            )
            .send()
            .join()

        zeebeClient.newWorker().jobType("test")
            .handler { jobClient, job ->
                jobClient.newCompleteCommand(job.key)
                    .variables(mapOf("x" to 1))
                    .send()
                    .join()
            }.open().use {

                // when
                val processInstanceResult =
                    zeebeClient.newCreateInstanceCommand().bpmnProcessId("process")
                        .latestVersion()
                        .withResult()
                        .send()
                        .join()

                // then
                assertThat(processInstanceResult.variablesAsMap).containsEntry("x", 1)
            }
    }

    @Test
    fun `should fail job`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn")
            .send()
            .join()

        zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        lateinit var job: ActivatedJob
        await.untilAsserted {
            val activateJobsResponse = zeebeClient
                .newActivateJobsCommand()
                .jobType("jobType")
                .maxJobsToActivate(32)
                .timeout(Duration.ofMinutes(1))
                .workerName("yolo")
                .fetchVariables(listOf("test"))
                .send()
                .join()

            val jobs = activateJobsResponse.jobs
            assertThat(jobs).isNotEmpty
            job = jobs[0]
        }

        // when - then
        zeebeClient.newFailCommand(job.key)
            .retries(0)
            .errorMessage("This failed oops.")
            .send()
            .join()

        await.untilAsserted {
            val failedJob = zeebeEngine
                .jobRecords()
                .withKey(job.key)
                .withIntent(JobIntent.FAILED)
                .firstOrNull()

            assertThat(failedJob).isNotNull
        }
    }

    @Test
    fun `should throw error on job`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .boundaryEvent("error")
                    .error("0xCAFE")
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn")
            .send()
            .join()

        zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        lateinit var job: ActivatedJob
        await.untilAsserted {
            val activateJobsResponse = zeebeClient
                .newActivateJobsCommand()
                .jobType("jobType")
                .maxJobsToActivate(32)
                .timeout(Duration.ofMinutes(1))
                .workerName("yolo")
                .fetchVariables(listOf("test"))
                .send()
                .join()

            val jobs = activateJobsResponse.jobs
            assertThat(jobs).isNotEmpty
            job = jobs[0]
        }

        // when - then
        zeebeClient.newThrowErrorCommand(job.key)
            .errorCode("0xCAFE")
            .errorMessage("What the fuck just happened.")
            .send()
            .join()

        await.untilAsserted {
            val boundaryEvent = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .withElementType(BpmnElementType.BOUNDARY_EVENT)
                .firstOrNull()

            assertThat(boundaryEvent).isNotNull
        }
    }


    @Test
    fun `should update retries on job`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn")
            .send()
            .join()

        zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        lateinit var job: ActivatedJob
        await.untilAsserted {
            val activateJobsResponse = zeebeClient
                .newActivateJobsCommand()
                .jobType("jobType")
                .maxJobsToActivate(32)
                .timeout(Duration.ofMinutes(1))
                .workerName("yolo")
                .fetchVariables(listOf("test"))
                .send()
                .join()

            val jobs = activateJobsResponse.jobs
            assertThat(jobs).isNotEmpty
            job = jobs[0]
        }

        // when - then
        zeebeClient.newUpdateRetriesCommand(job.key)
            .retries(3)
            .send()
            .join()

        await.untilAsserted {
            val retriesUpdated = zeebeEngine
                .jobRecords()
                .withKey(job.key)
                .withIntent(JobIntent.RETRIES_UPDATED)
                .firstOrNull()

            assertThat(retriesUpdated).isNotNull
        }
    }

    @Test
    fun `should read process instance records`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

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

        // when
        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
        await.untilAsserted {
            val processRecords = zeebeEngine
                .processInstanceRecords()
                .withRecordType(events = true)
                .withElementType(BpmnElementType.PROCESS)
                .take(4)

            assertThat(processRecords)
                .hasSize(4)
                .extracting<Intent> { it.intent }
                .contains(
                    ProcessInstanceIntent.ELEMENT_ACTIVATING,
                    ProcessInstanceIntent.ELEMENT_ACTIVATED,
                    ProcessInstanceIntent.ELEMENT_COMPLETING,
                    ProcessInstanceIntent.ELEMENT_COMPLETED
                )

            val processInstanceRecord = processRecords[0].value
            assertThat(processInstanceRecord.processDefinitionKey).isEqualTo(processInstance.processDefinitionKey)
            assertThat(processInstanceRecord.bpmnProcessId).isEqualTo(processInstance.bpmnProcessId)
            assertThat(processInstanceRecord.version).isEqualTo(processInstance.version)
            assertThat(processInstanceRecord.bpmnElementType).isEqualTo(BpmnElementType.PROCESS)
        }

    }

    @Test
    fun `should print records`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

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

        // when
        zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
        await.untilAsserted {
            val processRecords = zeebeEngine
                .processInstanceRecords()
                .withElementType(BpmnElementType.PROCESS)
                .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .firstOrNull()

            assertThat(processRecords).isNotNull
        }

        zeebeEngine.records().print()
    }

    @Test
    fun `should increase the time`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("process")
                    .startEvent()
                    .intermediateCatchEvent()
                    .timerWithDuration("P1D")
                    .endEvent()
                    .done(),
                "process.bpmn"
            )
            .send()
            .join()

        zeebeClient.newCreateInstanceCommand().bpmnProcessId("process")
            .latestVersion()
            .send()
            .join()

        await.untilAsserted {
            val timerCreated = zeebeEngine
                .timerRecords()
                .withIntent(TimerIntent.CREATED)
                .firstOrNull()

            assertThat(timerCreated).isNotNull
        }

        // when
        zeebeEngine.clock().increaseTime(Duration.ofDays(1))

        await.untilAsserted {
            val processCompleted = zeebeEngine
                .processInstanceRecords()
                .withElementType(BpmnElementType.PROCESS)
                .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .firstOrNull()

            assertThat(processCompleted).isNotNull
        }
    }

}
