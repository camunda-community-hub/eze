package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.deployment.distribute.DeploymentDistributor
import io.camunda.zeebe.util.sched.future.ActorFuture
import io.camunda.zeebe.util.sched.future.CompletableActorFuture
import org.agrona.DirectBuffer

class SinglePartitionDeploymentDistributor: DeploymentDistributor {

    override fun pushDeploymentToPartition(
        key: Long,
        partitionId: Int,
        deploymentBuffer: DirectBuffer?
    ): ActorFuture<Void> {
        // no need to implement if we have only one partition
        return CompletableActorFuture.completed(null);
    }
}
