package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.deployment.DeploymentResponder

class SinglePartitionDeploymentResponder: DeploymentResponder {

    override fun sendDeploymentResponse(deploymentKey: Long, partitionId: Int) {
        // no need to implement if there is only one partition
    }
}
