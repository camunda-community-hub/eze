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
import io.camunda.zeebe.client.api.worker.JobClient
import io.camunda.zeebe.model.bpmn.Bpmn
import io.camunda.zeebe.protocol.record.intent.*
import io.camunda.zeebe.protocol.record.value.BpmnElementType
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue
import io.camunda.zeebe.protocol.record.value.VariableRecordValue
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.groups.Tuple
import org.awaitility.kotlin.await
import org.camunda.community.eze.RecordStream.print
import org.camunda.community.eze.RecordStream.withElementId
import org.camunda.community.eze.RecordStream.withElementType
import org.camunda.community.eze.RecordStream.withIntent
import org.camunda.community.eze.RecordStream.withKey
import org.camunda.community.eze.RecordStream.withProcessInstanceKey
import org.camunda.community.eze.RecordStream.withRecordType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Duration


@ExtendWith(PrintRecordStreamExtension::class)
class EngineClientTest {

    private lateinit var zeebeEngine: ZeebeEngine
    private lateinit var zeebeClient: ZeebeClient

    @BeforeEach
    fun `setup grpc server`() {
        zeebeEngine = EngineFactory.create()
        zeebeEngine.start()

        PrintRecordStreamExtension.zeebeEngine = zeebeEngine
    }

    @AfterEach
    fun `tear down`() {
        zeebeEngine.stop()
        zeebeClient.close()
    }

    @Test
    fun `should request topology`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val topology = zeebeClient
            .newTopologyRequest()
            .send()
            .join()

        // then
        assertThat(topology.clusterSize).isEqualTo(1)
        assertThat(topology.replicationFactor).isEqualTo(1)
        assertThat(topology.partitionsCount).isEqualTo(1)
        assertThat(topology.gatewayVersion)
            .describedAs("Expect a version with the pattern 'dev (8.1.8)'")
            .containsPattern("""dev \(8\.\d+\.\d+(-.*)?\)""")

        assertThat(topology.brokers).hasSize(1)
        val broker = topology.brokers[0]
        assertThat(broker.host).isEqualTo("0.0.0.0")
        assertThat(broker.port).isEqualTo(26500)
        assertThat(broker.version)
            .describedAs("Expect a version with the pattern 'dev (8.1.8)'")
            .containsPattern("""dev \(8\.\d+\.\d+(-.*)?\)""")

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
        zeebeClient = zeebeEngine.createClient()

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
        zeebeClient = ZeebeClient
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient.newDeployResourceCommand()
            .addProcessModel(Bpmn.createExecutableProcess("process")
                .startEvent()
                .intermediateCatchEvent()
                .message { it.name("a").zeebeCorrelationKeyExpression("key") }
                .endEvent()
                .done(), "process.bpmn")
            .send()
            .join()

        val processInstanceResult = zeebeClient.newCreateInstanceCommand()
            .bpmnProcessId("process")
            .latestVersion()
            .variables(mapOf("key" to "key-1"))
            .withResult()
            .send()

        // when
        zeebeClient
            .newPublishMessageCommand()
            .messageName("a")
            .correlationKey("key-1")
            .variables(mapOf("message" to "correlated"))
            .send()
            .join()

        // then
        assertThat(processInstanceResult.join().variablesAsMap)
            .containsEntry("message", "correlated")
    }

    @Test
    fun `should deploy process`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

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
    fun `should deploy resources`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val deployment = zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
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
    fun `should create process instance with start instructions`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .userTask("A")
                    .userTask("B")
                    .userTask("C")
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()

        // when
        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .startBeforeElement("B")
            .startBeforeElement("C")
            .send()
            .join()

        // then
        assertThat(processInstance.processInstanceKey).isPositive
        assertThat(processInstance.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstance.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstance.version).isEqualTo(1)

        await.untilAsserted {
            val activatedUserTasks = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withElementType(BpmnElementType.USER_TASK)
                .take(2)


            assertThat(activatedUserTasks)
                .extracting<ProcessInstanceRecordValue> { it.value }
                .extracting<String> { it.elementId }
                .containsExactly("B", "C")
        }
    }

    @Test
    fun `should cancel process instance`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        val deployment = zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
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
    fun `should fail job with backoff`() {
        // given
        val backoffDuration = Duration.ofMinutes(5)

        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task") { it.zeebeJobType("jobType") }
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn")
            .send()
            .join()

        val processInstanceKey =
            zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
                .latestVersion()
                .send()
                .join()
                .processInstanceKey

        var jobKey: Long = -1
        await.untilAsserted {
            val createdJob = zeebeEngine
                .jobRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withIntent(JobIntent.CREATED)
                .firstOrNull()

            assertThat(createdJob).isNotNull
            jobKey = createdJob!!.key
        }

        // when
        zeebeClient.newFailCommand(jobKey)
            .retries(2)
            .errorMessage("let's try again")
            .retryBackoff(backoffDuration)
            .send()
            .join()

        // then
        await.untilAsserted {
            val failedJob = zeebeEngine
                .jobRecords()
                .withKey(jobKey)
                .withIntent(JobIntent.FAILED)
                .firstOrNull()

            assertThat(failedJob).isNotNull
            assertThat(failedJob!!.value.retryBackoff).isEqualTo(backoffDuration.toMillis())
        }
    }

    @Test
    fun `should throw error on job`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
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
    fun `should modify process instance`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .userTask("A")
                    .userTask("B")
                    .userTask("C")
                    .endEvent()
                    .done(),
                "simpleProcess.bpmn"
            )
            .send()
            .join()

        val processInstanceKey =
            zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
                .latestVersion()
                .send()
                .join()
                .processInstanceKey

        var instanceKeyOfElementA: Long = -1
        await.untilAsserted {
            val activatedTask = zeebeEngine
                .processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withElementId("A")
                .firstOrNull()

            assertThat(activatedTask).isNotNull
            instanceKeyOfElementA = activatedTask!!.key
        }

        // when
        zeebeClient.newModifyProcessInstanceCommand(processInstanceKey)
            .terminateElement(instanceKeyOfElementA)
            .and()
            .activateElement("B")
            .withVariables(mapOf("global" to 1))
            .withVariables(mapOf("localB" to 2), "B")
            .and()
            .activateElement("C")
            .withVariables(mapOf("localC" to 3), "C")
            .send()
            .join()

        // then
        await.untilAsserted {
            val terminatedUserTask = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withElementType(BpmnElementType.USER_TASK)
                .firstOrNull()

            assertThat(terminatedUserTask).isNotNull
            assertThat(terminatedUserTask!!.value.elementId).isEqualTo("A")

            val activatedUserTasks = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
                .withElementType(BpmnElementType.USER_TASK)
                .take(3)

            assertThat(activatedUserTasks)
                .extracting<ProcessInstanceRecordValue> { it.value }
                .extracting<String> { it.elementId }
                .containsExactly("A", "B", "C")

            val variables = zeebeEngine.variableRecords()
                .withProcessInstanceKey(processInstanceKey)
                .take(3)

            assertThat(variables)
                .extracting<VariableRecordValue> { it.value }
                .extracting({ it.scopeKey }, { it.name }, { it.value })
                .containsExactly(
                    Tuple.tuple(processInstanceKey, "global", "1"),
                    Tuple.tuple(activatedUserTasks[1].key, "localB", "2"),
                    Tuple.tuple(activatedUserTasks[2].key, "localC", "3")
                )
        }
    }

    @Test
    fun `should read process instance records`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployResourceCommand()
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
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient
            .newDeployResourceCommand()
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

    // regression test https://github.com/camunda-cloud/zeebe-process-test/issues/158
    @Test
    @Throws(InterruptedException::class)
    fun testJobsCanBeProcessedAsynchronouslyByWorker() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newWorker()
            .jobType("jobType")
            .handler { client: JobClient, job: ActivatedJob ->
                client.newCompleteCommand(job).send()
            }
            .open()
            .use {
                zeebeClient
                    .newDeployResourceCommand()
                    .addProcessModel(
                        Bpmn.createExecutableProcess("simpleProcess")
                            .startEvent()
                            .serviceTask("task") {
                                it.zeebeJobType("jobType").multiInstance().parallel()
                                    .zeebeInputElement("idx").zeebeInputCollection("=indexes")
                            }
                            .endEvent()
                            .done(),
                        "simpleProcess.bpmn"
                    )
                    .send()
                    .join()

                // when
                val processInstance =
                    zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
                        .latestVersion()
                        .variables(mapOf("indexes" to listOf(1, 2, 3)))
                        .withResult()
                        .send()
                        .join()


                // then
                await.untilAsserted {
                    val processInstanceRecord = zeebeEngine
                        .processInstanceRecords()
                        .withRecordType(events = true)
                        .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                        .withElementType(BpmnElementType.PROCESS)
                        .firstOrNull()!!.value

                    assertThat(processInstanceRecord.processDefinitionKey).isEqualTo(processInstance.processDefinitionKey)
                    assertThat(processInstanceRecord.bpmnProcessId).isEqualTo(processInstance.bpmnProcessId)
                    assertThat(processInstanceRecord.version).isEqualTo(processInstance.version)
                    assertThat(processInstanceRecord.bpmnElementType).isEqualTo(BpmnElementType.PROCESS)
                }
            }
    }

    @Test
    fun `should evaluate decision`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        zeebeClient
            .newDeployResourceCommand()
            .addResourceFromClasspath("rating.dmn")
            .send()
            .join()

        // when
        val decisionEvaluation =
            zeebeClient.newEvaluateDecisionCommand()
                .decisionId("decision_a")
                .variables(mapOf("x" to 7))
                .send()
                .join()

        // then
        assertThat(decisionEvaluation.decisionId).isEqualTo("decision_a")
        assertThat(decisionEvaluation.decisionName).isEqualTo("Decision A")
        assertThat(decisionEvaluation.decisionVersion).isEqualTo(1)
        assertThat(decisionEvaluation.decisionOutput).isEqualTo("\"A+\"")
        assertThat(decisionEvaluation.evaluatedDecisions).hasSize(2)
    }

    // Disable delete resource https://github.com/camunda/zeebe/pull/12111
    @Disabled
    @Test
    fun `should delete resources`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployResourceCommand()
            .addResourceFromClasspath("rating.dmn")
            .send()
            .join()

        val drg = deployment.decisionRequirements.first()
        assertThat(drg).isNotNull

        // when
        zeebeClient
            .newDeleteResourceCommand(drg.decisionRequirementsKey)
            .send()
            .join()

        // then
        await.untilAsserted {
            val drgDeleted =
                zeebeEngine.decisionRequirementsRecords()
                    .withIntent(DecisionRequirementsIntent.DELETED).first()

            assertThat(drgDeleted).isNotNull
            assertThat(drgDeleted.value.decisionRequirementsKey).isEqualTo(drg.decisionRequirementsKey)
        }
    }

    @Test
    fun `should broadcast signal`() {
        // given
        zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        zeebeClient.newDeployResourceCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("process")
                    .startEvent()
                    .signal("top-level-start-signal")
                    .endEvent()
                    .done(), "process.bpmn"
            )
            .send()
            .join()

        // when
        zeebeClient
            .newBroadcastSignalCommand()
            .signalName("top-level-start-signal")
            .variables(mapOf("signal" to "broadCasted"))
            .send()
            .join()

        // then
        await.untilAsserted {
            val processInstance = zeebeEngine
                .processInstanceRecords()
                .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .withElementType(BpmnElementType.PROCESS)
                .firstOrNull()

            assertThat(processInstance)
                .describedAs("The top-level signal start event can be triggered by signal broadcast command")
                .isNotNull
        }

    }
}
