/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.protocol.record.Record
import io.camunda.zeebe.scheduler.clock.ControlledActorClock
import org.camunda.community.eze.ZeebeEngine
import org.camunda.community.eze.ZeebeEngineClock

class ZeebeEngineImpl(
    val startCallback: Runnable,
    val stopCallback: Runnable,
    val recordStream: () -> Iterable<Record<*>>,
    val clock: ControlledActorClock
) : ZeebeEngine {

    companion object {
        const val PORT = 26500
    }

    override fun start() {
        startCallback.run()
    }

    override fun stop() {
        stopCallback.run()
    }

    override fun records(): Iterable<Record<*>> {
        return recordStream.invoke()
    }

    override fun createClient(): ZeebeClient {
        return ZeebeClient.newClientBuilder().usePlaintext().build()
    }

    override fun getGatewayAddress(): String {
        return "0.0.0.0:$PORT"
    }

    override fun clock(): ZeebeEngineClock {
        return ZeebeEngineClockImpl(clock = clock)
    }
}
