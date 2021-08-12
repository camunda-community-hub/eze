package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient
import io.camunda.zeebe.protocol.record.Record

class ZeebeEngineImpl(
    val startCallback: Runnable,
    val stopCallback: Runnable,
    val recordStream: () -> Iterable<Record<*>>
) : ZeebeEngine {

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
}
