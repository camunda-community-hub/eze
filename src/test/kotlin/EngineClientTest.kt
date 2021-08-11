import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.util.MutableHandlerRegistry
import org.assertj.core.api.Assertions.assertThat
import org.camunda.community.eze.EngineFactory
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

class EngineClientTest {

    @BeforeEach
    fun setupGrpcServer() {
        val zeebeEngine = EngineFactory.create()
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
