/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine;

import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RecordValue;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.PostCommitTask;
import io.camunda.zeebe.stream.api.ProcessingResponse;
import io.camunda.zeebe.stream.api.ProcessingResult;
import io.camunda.zeebe.stream.api.ProcessingResultBuilder;
import io.camunda.zeebe.stream.api.records.RecordBatchSizePredicate;
import io.camunda.zeebe.stream.impl.TypedEventRegistry;
import io.camunda.zeebe.stream.impl.records.RecordBatch;
import io.camunda.zeebe.stream.impl.records.RecordBatchEntry;
import io.camunda.zeebe.util.Either;
import io.camunda.zeebe.util.StringUtil;
import java.util.ArrayList;
import java.util.List;

final class BufferedProcessingResultBuilder implements ProcessingResultBuilder {
  private final List<PostCommitTask> postCommitTasks = new ArrayList();
  private final RecordBatch mutableRecordBatch;
  private BufferedProcessingResultBuilder.ProcessingResponseImpl processingResponse;

  BufferedProcessingResultBuilder(final RecordBatchSizePredicate predicate) {
    this.mutableRecordBatch = new RecordBatch(predicate);
  }

  public Either<RuntimeException, ProcessingResultBuilder> appendRecordReturnEither(
      final long key,
      final RecordType type,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final RecordValue value) {
    ValueType valueType = (ValueType) TypedEventRegistry.TYPE_REGISTRY.get(value.getClass());
    if (valueType == null) {
      throw new IllegalStateException("Missing value type mapping for record: " + value.getClass());
    } else if (value instanceof UnifiedRecordValue) {
      UnifiedRecordValue unifiedRecordValue = (UnifiedRecordValue) value;
      Either<RuntimeException, Void> either =
          this.mutableRecordBatch.appendRecord(
              key, -1, type, intent, rejectionType, rejectionReason, valueType, unifiedRecordValue);
      return either.isLeft()
          ? Either.left((RuntimeException) either.getLeft())
          : Either.right(this);
    } else {
      throw new IllegalStateException(
          String.format(
              "The record value %s is not a UnifiedRecordValue",
              StringUtil.limitString(value.toString(), 1024)));
    }
  }

  public ProcessingResultBuilder withResponse(
      final RecordType recordType,
      final long key,
      final Intent intent,
      final UnpackedObject value,
      final ValueType valueType,
      final RejectionType rejectionType,
      final String rejectionReason,
      final long requestId,
      final int requestStreamId) {
    RecordBatchEntry entry =
        RecordBatchEntry.createEntry(
            key, -1, recordType, intent, rejectionType, rejectionReason, valueType, value);
    this.processingResponse =
        new BufferedProcessingResultBuilder.ProcessingResponseImpl(
            entry, requestId, requestStreamId);
    return this;
  }

  public ProcessingResultBuilder appendPostCommitTask(final PostCommitTask task) {
    this.postCommitTasks.add(task);
    return this;
  }

  public ProcessingResultBuilder resetPostCommitTasks() {
    this.postCommitTasks.clear();
    return this;
  }

  public ProcessingResult build() {
    return new BufferedResult(
        this.mutableRecordBatch, this.processingResponse, this.postCommitTasks);
  }

  public boolean canWriteEventOfLength(final int eventLength) {
    return this.mutableRecordBatch.canAppendRecordOfLength(eventLength);
  }

  record ProcessingResponseImpl(RecordBatchEntry responseValue, long requestId, int requestStreamId)
      implements ProcessingResponse {

    public RecordBatchEntry responseValue() {
      return this.responseValue;
    }

    public long requestId() {
      return this.requestId;
    }

    public int requestStreamId() {
      return this.requestStreamId;
    }
  }
}
