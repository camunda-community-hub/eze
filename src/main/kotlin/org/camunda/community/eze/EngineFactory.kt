package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.util.sched.ActorScheduler
import io.camunda.zeebe.util.sched.clock.ActorClock
import io.camunda.zeebe.util.sched.clock.ControlledActorClock

object EngineFactory {

    fun create(): ZeebeEngine {

        val clock = createActorClock()
        val scheduler = createActorScheduler(clock)

        TODO("create an engine")
    }

    private fun createStreamProcessor(): StreamProcessor {
        return StreamProcessor.builder()
            .build()
    }

    private fun createLogStream(partitionId: Int, logStorage: LogStorage, scheduler: ActorScheduler): LogStream {
        return LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withActorScheduler(scheduler)
            .buildAsync()
            .join()
    }

    private fun createLogStorage(): LogStorage {
        TODO ("create log storage")
    }

    private fun createActorScheduler(clock: ActorClock): ActorScheduler {
        return ActorScheduler.newActorScheduler()
            .setActorClock(clock)
            .build()
    }

    private fun createActorClock(): ControlledActorClock {
        return ControlledActorClock()
    }

}
