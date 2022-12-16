/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine

import io.camunda.zeebe.logstreams.log.LogStream
import io.camunda.zeebe.logstreams.storage.LogStorage
import io.camunda.zeebe.scheduler.Actor
import io.camunda.zeebe.scheduler.ActorSchedulingService
import java.util.concurrent.CompletableFuture

private const val LOG_STREAM_NAME = "EZE-LOG"

