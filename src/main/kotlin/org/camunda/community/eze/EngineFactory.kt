package org.camunda.community.eze

import io.camunda.zeebe.db.ZeebeDb
import io.camunda.zeebe.engine.processing.EngineProcessors
import io.camunda.zeebe.engine.processing.deployment.DeploymentResponder
import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributor
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.engine.processing.streamprocessor.StreamProcessor
import io.camunda.zeebe.engine.state.DefaultZeebeDbFactory
import io.camunda.zeebe.engine.state.ZbColumnFamilies
import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.util.sched.Actor
import io.camunda.zeebe.util.sched.ActorScheduler
import io.camunda.zeebe.util.sched.ActorSchedulingService
import io.camunda.zeebe.util.sched.clock.ActorClock
import io.camunda.zeebe.util.sched.clock.ControlledActorClock
import java.nio.file.Files
import java.util.concurrent.CompletableFuture

object EngineFactory {

    fun create(): ZeebeEngine {

        val clock = createActorClock()
        val scheduler = createActorScheduler(clock)

        val logStorage = createLogStorage()
        val logStream = createLogStream(partitionId = 1, logStorage = logStorage, scheduler = scheduler)

        val db = createDatabase()



        TODO("create an engine")
    }

    private fun createStreamProcessor(
        partitionCount: Int,
        logStream: LogStream,
        database: ZeebeDb<ZbColumnFamilies>,
        scheduler: ActorSchedulingService
    ): StreamProcessor {
        return StreamProcessor.builder()
            .logStream(logStream)
            .zeebeDb(database)
            .streamProcessorFactory { context ->
                EngineProcessors.createEngineProcessors(
                    context,
                    partitionCount,
                    createSubscriptionCommandSender(),
                    createDeploymentDistributor(),
                    createDeploymentResponder(),
                    this::createJobsAvailableCallback
                )
            }
            .actorSchedulingService(scheduler)
            .build()
    }

    private fun createSubscriptionCommandSender(): SubscriptionCommandSender {
        TODO()
    }

    private fun createDeploymentDistributor(): DeploymentDistributor {
        TODO()
    }

    private fun createDeploymentResponder(): DeploymentResponder {
        TODO()
    }

    private fun createJobsAvailableCallback(jobType: String) {
        // new job available
    }

    private fun createDatabase(): ZeebeDb<ZbColumnFamilies> {
        val zeebeDbFactory = DefaultZeebeDbFactory.defaultFactory()
        return zeebeDbFactory.createDb(Files.createTempDirectory("zeebeDb").toFile())
    }

    private fun createLogStream(partitionId: Int, logStorage: LogStorage, scheduler: ActorSchedulingService): LogStream {
        val builder = LogStream.builder()
            .withPartitionId(partitionId)
            .withLogStorage(logStorage)
            .withActorSchedulingService(scheduler)

        val theFuture = CompletableFuture<LogStream>()

        scheduler.submitActor(Actor.wrap {
            builder
                .buildAsync()
                .onComplete { logStream, failure ->
                    // TODO boom
                    theFuture.complete(logStream)
                }
        })

        return theFuture.join()
    }

    private fun createLogStorage(): LogStorage {
        return InMemoryLogStorage()
    }

    private fun createActorScheduler(clock: ActorClock): ActorSchedulingService {
        val scheduler = ActorScheduler.newActorScheduler()
            .setActorClock(clock)
            .build()

        scheduler.start()

        return scheduler
    }

    private fun createActorClock(): ControlledActorClock {
        return ControlledActorClock()
    }

}
