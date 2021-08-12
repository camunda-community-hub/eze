/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.deployment.DeploymentResponder

class SinglePartitionDeploymentResponder: DeploymentResponder {

    override fun sendDeploymentResponse(deploymentKey: Long, partitionId: Int) {
        // no need to implement if there is only one partition
    }
}
