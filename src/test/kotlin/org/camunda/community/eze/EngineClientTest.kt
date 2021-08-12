package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.client.api.response.ActivatedJob
import io.camunda.zeebe.model.bpmn.Bpmn
import io.camunda.zeebe.protocol.record.intent.Intent
import io.camunda.zeebe.protocol.record.intent.JobIntent
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent
import io.camunda.zeebe.protocol.record.value.BpmnElementType
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.camunda.community.eze.RecordStream.intent
import org.camunda.community.eze.RecordStream.job
import org.camunda.community.eze.RecordStream.key
import org.camunda.community.eze.RecordStream.ofElementType
import org.camunda.community.eze.RecordStream.ofRecordType
import org.camunda.community.eze.RecordStream.print
import org.camunda.community.eze.RecordStream.processInstance
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
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

    @Disabled
    // TODO handle rejections
    fun `should fail on create process instance`() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()

        // when
        val processInstance = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
            .latestVersion()
            .variables(mapOf("test" to 1))
            .send()
            .join()

        // then
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
            }.open()

        // when
        val processInstanceResult = zeebeClient.newCreateInstanceCommand().bpmnProcessId("process")
            .latestVersion()
            .withResult()
            .send()
            .join()

        // then
        assertThat(processInstanceResult.variablesAsMap).containsEntry("x", 1)
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
            val failedJob = zeebeEngine.records()
                .job()
                .key(job.key)
                .intent(JobIntent.FAILED)
                .firstOrNull()

            assertThat(failedJob).isNotNull
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
            val boundaryEvent = zeebeEngine.records()
                .processInstance()
                .intent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .ofElementType(BpmnElementType.BOUNDARY_EVENT)
                .firstOrNull()

            assertThat(boundaryEvent).isNotNull
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

        // TODO add assert - after records are available
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
            val processRecords = zeebeEngine.records()
                .processInstance()
                .ofRecordType(events = true)
                .ofElementType(BpmnElementType.PROCESS)
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
            val processRecords = zeebeEngine.records()
                .processInstance()
                .ofElementType(BpmnElementType.PROCESS)
                .intent(ProcessInstanceIntent.ELEMENT_COMPLETED)
                .firstOrNull()

            assertThat(processRecords).isNotNull
        }

        zeebeEngine.records().print()
    }

}
