package org.camunda.community.eze.engine

class EzeStreamProcessor(
    private val startCallback: Runnable,
    private val stopCallback: Runnable
) {

    fun start() {
        startCallback.run()
    }

    fun stop() {
        stopCallback.run()
    }

}
