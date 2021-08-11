package org.camunda.community.eze

import io.grpc.Server
import java.util.concurrent.Future

class ZeebeEngineImpl(val startCallback: Runnable, val stopCallback: Runnable) :ZeebeEngine {

    override fun start() {
        startCallback.run()
    }

    override fun stop() {
        stopCallback.run()
    }
}
