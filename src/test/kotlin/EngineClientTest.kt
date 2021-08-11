import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.gateway.protocol.GatewayGrpc
import io.camunda.zeebe.gateway.protocol.GatewayOuterClass
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.grpc.util.MutableHandlerRegistry
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

class EngineClientTest {

    class ExampleGateway : GatewayGrpc.GatewayImplBase() {
        override fun publishMessage(
            request: GatewayOuterClass.PublishMessageRequest,
            responseObserver: StreamObserver<GatewayOuterClass.PublishMessageResponse>
        ) {
            responseObserver.onNext(GatewayOuterClass.PublishMessageResponse.getDefaultInstance())
            responseObserver.onCompleted()
        }
    }

    @BeforeEach
    fun setupGrpcServer() {
        val server = ServerBuilder.forPort(26500).addService(ExampleGateway()).build()
        server.start()
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

    }
}
