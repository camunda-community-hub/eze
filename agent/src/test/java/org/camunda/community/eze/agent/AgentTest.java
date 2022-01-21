/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.agent;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.Topology;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.camunda.community.eze.ZeebeEngineImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import sun.misc.Signal;

final class AgentTest {
  private ExecutorService agentThreadContext;

  @AfterEach
  void afterEach() {
    if (agentThreadContext != null) {
      agentThreadContext.shutdownNow();
    }
  }

  @Test
  void shouldStartEngine() {
    // given
    final var agent = new Agent();
    agentThreadContext = Executors.newSingleThreadExecutor();

    // when
    final var exitCodeFuture = agentThreadContext.submit(agent);
    final Topology topology;
    try (final var client =
        ZeebeClient.newClientBuilder()
            .usePlaintext()
            .gatewayAddress("localhost:" + ZeebeEngineImpl.PORT)
            .build()) {
      topology = client.newTopologyRequest().send().join();
    }
    Signal.raise(new Signal("INT"));

    // then
    assertThat(topology.getClusterSize()).isEqualTo(1);
    assertThat(exitCodeFuture)
        .succeedsWithin(Duration.ofSeconds(10))
        .isEqualTo(ExitCode.OK.getCode());
  }
}
