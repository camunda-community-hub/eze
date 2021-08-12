/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
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
