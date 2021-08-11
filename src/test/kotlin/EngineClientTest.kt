import io.camunda.zeebe.client.ZeebeClient
import org.assertj.core.api.Assertions.assertThat
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
}
