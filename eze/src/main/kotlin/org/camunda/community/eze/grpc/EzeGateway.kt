package org.camunda.community.eze.grpc

import io.camunda.zeebe.engine.api.CommandResponseWriter

class EzeGateway(
    private val responseWriter: GrpcResponseWriter,
    private val startCallback: Runnable,
    private val stopCallback: Runnable
) {

    fun start() {
        startCallback.run()
    }

    fun stop() {
        stopCallback.run()
    }

    fun getResponseWriter(): CommandResponseWriter = responseWriter

}
