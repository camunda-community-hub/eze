/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.grpc

import io.camunda.zeebe.stream.api.CommandResponseWriter


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
