/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandMessageHandler
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender
import io.camunda.zeebe.logstreams.log.LogStreamRecordWriter
import java.util.concurrent.Executors

class SubscriptionCommandSenderFactory(
    private val writerLookUp: (Int) -> LogStreamRecordWriter
) {

    private val subscriptionHandlers = mutableMapOf<Int, SubscriptionCommandMessageHandler>()

    private val subscriptionHandlerExecutor = Executors.newSingleThreadExecutor()

    fun ofPartition(partitionId: Int): SubscriptionCommandSender {

        val handler = SubscriptionCommandMessageHandler(
            subscriptionHandlerExecutor::submit,
            writerLookUp
        )

        subscriptionHandlers[partitionId] = handler

        return SubscriptionCommandSender(partitionId,
            PartitionCommandSenderImpl { receiverPartitionId, message ->
                subscriptionHandlers[receiverPartitionId].let { it.apply { message } }
            })
    }

}
