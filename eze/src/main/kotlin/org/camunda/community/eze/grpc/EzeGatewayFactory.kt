/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
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
