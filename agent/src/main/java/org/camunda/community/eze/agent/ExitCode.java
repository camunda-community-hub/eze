/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.agent;

/**
 * A list of possible exit status codes returned by the Agent. These can be consumed by scripts or
 * container orchestrators to react appropriately based on the error.
 */
enum ExitCode {
  /** Indicates the application started and shutdown gracefully without errors */
  OK(0),
  /** Indicates the agent failed to create an instance of EZE */
  CREATE_ERROR(128),
  /** Indicates the agent failed to start the EZE instance */
  START_ERROR(129),
  /** Indicates the agent started the engine, but failed to gracefully shut it down */
  SHUTDOWN_ERROR(131);

  private final int code;

  ExitCode(final int code) {
    this.code = code;
  }

  int getCode() {
    return code;
  }
}
