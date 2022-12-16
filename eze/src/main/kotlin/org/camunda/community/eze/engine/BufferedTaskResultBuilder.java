/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.engine;

import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.stream.api.records.MutableRecordBatch;
import io.camunda.zeebe.stream.api.records.RecordBatchSizePredicate;
import io.camunda.zeebe.stream.api.scheduling.TaskResult;
import io.camunda.zeebe.stream.api.scheduling.TaskResultBuilder;
import io.camunda.zeebe.stream.impl.TypedEventRegistry;
import io.camunda.zeebe.stream.impl.records.RecordBatch;
import io.camunda.zeebe.util.Either;

final class BufferedTaskResultBuilder implements TaskResultBuilder {
  private final MutableRecordBatch mutableRecordBatch;

  BufferedTaskResultBuilder(final RecordBatchSizePredicate predicate) {
    this.mutableRecordBatch = new RecordBatch(predicate);
  }

  public boolean appendCommandRecord(final long key, final Intent intent, final UnifiedRecordValue value) {
    ValueType valueType = (ValueType) TypedEventRegistry.TYPE_REGISTRY.get(value.getClass());
    if (valueType == null) {
      throw new IllegalStateException("Missing value type mapping for record: " + value.getClass());
    } else {
      Either<RuntimeException, Void> either = this.mutableRecordBatch.appendRecord(key, -1, RecordType.COMMAND, intent, RejectionType.NULL_VAL, "", valueType, value);
      return either.isRight();
    }
  }

  public TaskResult build() {
    return () -> {
      return this.mutableRecordBatch;
    };
  }
}
