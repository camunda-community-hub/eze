/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.client.ZeebeClient

interface ZeebeEngine : RecordStreamSource {

    fun start()

    fun stop()

    fun createClient(): ZeebeClient

    fun getGatewayAddress(): String

    fun clock(): ZeebeEngineClock
}
