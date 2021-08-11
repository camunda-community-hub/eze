import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.model.bpmn.Bpmn
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple
import org.camunda.community.eze.EngineFactory
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class EngineClientTest {

    @BeforeEach
    fun setupGrpcServer() {
        EngineFactory.create()
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
}
