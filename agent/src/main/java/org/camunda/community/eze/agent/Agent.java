/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.agent;

import java.util.Collections;
import java.util.concurrent.Callable;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.camunda.community.eze.EngineFactory;
import org.camunda.community.eze.ZeebeEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * A thin wrapper around a {@link ZeebeEngine} instance. For potential exit codes, see {@link
 * ExitCode}.
 */
@Command(name = "agent", mixinStandardHelpOptions = true, description = "Standalone EZE agent")
public final class Agent implements Callable<Integer> {

  private final Logger logger;

  public Agent() {
    logger = LoggerFactory.getLogger(getClass());
  }

  @Override
  public Integer call() {
    final var shutdownBarrier = new ShutdownSignalBarrier();
    final ZeebeEngine engine;

    logger.info("Starting EZE agent...");
    try {
      engine = EngineFactory.INSTANCE.create(Collections.emptyList());
    } catch (final Exception e) {
      logger.error("Failed to create an instance of the embedded engine", e);
      return ExitCode.CREATE_ERROR.getCode();
    }

    try {
      engine.start();
    } catch (final Exception e) {
      logger.error("Failed to start EZE agent", e);
      return ExitCode.START_ERROR.getCode();
    }

    logger.info("EZE agent started at {}", engine.getGatewayAddress());
    ExitCode exitCode = ExitCode.OK;
    shutdownBarrier.await();

    logger.info("Shutting down EZE agent...");
    try {
      engine.stop();
    } catch (final Exception e) {
      logger.warn("Failed to gracefully shutdown the embedded engine", e);
      exitCode = ExitCode.SHUTDOWN_ERROR;
    }

    logger.info("Shutdown EZE agent with status: {}", exitCode);
    return exitCode.getCode();
  }

  public static void main(final String[] args) {
    final int exitCode = new CommandLine(new Agent()).execute(args);
    System.exit(exitCode);
  }
}
