package org.camunda.community.eze.grpc

import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import io.grpc.ServerBuilder

object EzeGatewayFactory {

    fun createGateway(
        port: Int,
        streamWriter: LogStreamRecordWriter
    ): EzeGateway {

        val gateway = GrpcToLogStreamGateway(writer = streamWriter)
        val grpcServer = ServerBuilder.forPort(port).addService(gateway).build()

        val grpcResponseWriter = GrpcResponseWriter(
            responseCallback = gateway::responseCallback,
            errorCallback = gateway::errorCallback,
            expectedResponse = gateway::getExpectedResponseType
        )

        return EzeGateway(
            startCallback = { grpcServer.start() },
            stopCallback = {
                grpcServer.shutdownNow()
                grpcServer.awaitTermination()
                gateway.close()
            },
            responseWriter = grpcResponseWriter
        )
    }

}
