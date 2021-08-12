import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.model.bpmn.Bpmn
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.camunda.community.eze.EngineFactory
import org.camunda.community.eze.ZeebeEngine
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

class EngineClientTest {

    lateinit var zeebeEngine : ZeebeEngine

    @BeforeEach
    fun setupGrpcServer() {
        zeebeEngine = EngineFactory.create()
        zeebeEngine.start()
    }

    @AfterEach
    fun tearDown() {
        zeebeEngine.stop()
    }

    @Test
    fun shouldPublishMessage() {
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
        assertThat(message.messageKey).isPositive;
    }

    @Test
    fun shouldDeployProcess() {
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
        assertThat(deployment.key).isPositive;
        assertThat(deployment.processes).isNotEmpty

        val process = deployment.processes[0]

        assertThat(process.version).isEqualTo(1)
        assertThat(process.resourceName).isEqualTo("simpleProcess.bpmn")
        assertThat(process.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(process.processDefinitionKey).isPositive()
    }

    @Test
    fun shouldCreateInstanceWithoutVariables() {
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
        assertThat(processInstance.processInstanceKey).isPositive;
        assertThat(processInstance.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstance.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstance.version).isEqualTo(1)
    }

    @Test
    fun shouldCreateInstance() {
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
        assertThat(processInstance.processInstanceKey).isPositive;
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
        assertThat(processInstanceResult.processInstanceKey).isPositive;
        assertThat(processInstanceResult.bpmnProcessId).isEqualTo("simpleProcess")
        assertThat(processInstanceResult.processDefinitionKey).isEqualTo(deployment.processes[0].processDefinitionKey)
        assertThat(processInstanceResult.version).isEqualTo(1)
        assertThat(processInstanceResult.variablesAsMap).containsEntry("test", 1)
    }

    @Test
    fun shouldActivateJob() {
        // given
        val zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()
        val deployment = zeebeClient
            .newDeployCommand()
            .addProcessModel(
                Bpmn.createExecutableProcess("simpleProcess")
                    .startEvent()
                    .serviceTask("task", { it.zeebeJobType("jobType") } )
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
                    .serviceTask("task", { it.zeebeJobType("test") } )
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
}
