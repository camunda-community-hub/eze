package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor

object EngineFactory {

    fun create(): ZeebeEngine {
        TODO("create an engine")
    }

    private fun createStreamProcessor(): StreamProcessor {
        return StreamProcessor.builder()
            .build()
    }

}
