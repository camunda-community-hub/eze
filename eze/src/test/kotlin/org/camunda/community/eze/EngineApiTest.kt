/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit


class EngineApiTest {

    private lateinit var zeebeEngine: ZeebeEngine

    @BeforeEach
    fun `setup server`() {
        zeebeEngine = EngineFactory.create()
        zeebeEngine.start()
    }

    @AfterEach
    fun `tear down`() {
        zeebeEngine.stop()
    }

    @Test
    fun `should get current time`() {
        // given
        val now = Instant.now()

        // when
        val currentTime = zeebeEngine.clock().getCurrentTime()

        // then
        assertThat(currentTime).isCloseTo(now, within(1, ChronoUnit.SECONDS))
    }

    @Test
    fun `should increase time`() {
        // given
        val timeBefore = zeebeEngine.clock().getCurrentTime()
        val duration = Duration.ofHours(1)

        // when
        zeebeEngine.clock().increaseTime(duration)

        val timeAfter = zeebeEngine.clock().getCurrentTime()

        // then
        assertThat(timeAfter).isCloseTo(timeBefore.plus(duration), within(1, ChronoUnit.SECONDS))
    }

}
