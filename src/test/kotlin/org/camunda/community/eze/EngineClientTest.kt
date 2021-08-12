package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.client.api.response.ActivateJobsResponse
import io.camunda.zeebe.client.api.response.ActivatedJob
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.camunda.zeebe.model.bpmn.Bpmn
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class EngineClientTest {

    private lateinit var zeebeEngine : ZeebeEngine

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
                "simpleProcess.bpmn")
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
                "simpleProcess.bpmn")
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
                "simpleProcess.bpmn")
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
                "simpleProcess.bpmn")
            .send()
            .join()

        // when
        val processInstanceResult = zeebeClient.newCreateInstanceCommand().bpmnProcessId("simpleProcess")
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
                "simpleProcess.bpmn")
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
                "process.bpmn")
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

        lateinit var job : ActivatedJob
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

        // when
        val failJobResponse = zeebeClient.newFailCommand(job.key)
            .retries(0)
            .errorMessage("This failed oops.")
            .send()
            .join()

        // then
        assertThat(failJobResponse).isNotNull
    }
}
