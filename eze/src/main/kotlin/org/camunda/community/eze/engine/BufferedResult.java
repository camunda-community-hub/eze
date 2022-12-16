/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine;

import io.camunda.zeebe.stream.api.PostCommitTask;
import io.camunda.zeebe.stream.api.ProcessingResponse;
import io.camunda.zeebe.stream.api.ProcessingResult;
import io.camunda.zeebe.stream.api.records.ImmutableRecordBatch;
import io.camunda.zeebe.stream.api.scheduling.TaskResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class BufferedResult implements ProcessingResult, TaskResult {
  private final List<PostCommitTask> postCommitTasks;
  private final ImmutableRecordBatch immutableRecordBatch;
  private final BufferedProcessingResultBuilder.ProcessingResponseImpl processingResponse;

  BufferedResult(
      final ImmutableRecordBatch immutableRecordBatch,
      final BufferedProcessingResultBuilder.ProcessingResponseImpl processingResponse,
      final List<PostCommitTask> postCommitTasks) {
    this.postCommitTasks = new ArrayList<>(postCommitTasks);
    this.processingResponse = processingResponse;
    this.immutableRecordBatch = immutableRecordBatch;
  }

  public ImmutableRecordBatch getRecordBatch() {
    return this.immutableRecordBatch;
  }

  public Optional<ProcessingResponse> getProcessingResponse() {
    return Optional.ofNullable(this.processingResponse);
  }

  public boolean executePostCommitTasks() {
    boolean aggregatedResult = true;

    for (final PostCommitTask task : this.postCommitTasks) {
      try {
        aggregatedResult = aggregatedResult && task.flush();
      } catch (Exception var5) {
        throw new RuntimeException(var5);
      }
    }

    return aggregatedResult;
  }
}
