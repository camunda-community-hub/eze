/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import io.camunda.zeebe.scheduler.clock.ControlledActorClock
import java.time.Duration
import java.time.Instant

class ZeebeEngineClockImpl(val clock: ControlledActorClock) : ZeebeEngineClock {

    override fun increaseTime(timeToAdd: Duration) {
        clock.addTime(timeToAdd)
    }

    override fun getCurrentTime(): Instant {
        return clock.currentTime
    }
}
