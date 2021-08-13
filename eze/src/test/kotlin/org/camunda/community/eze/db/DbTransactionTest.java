/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.ZeebeDbFactory;
import io.camunda.zeebe.db.ZeebeDbTransaction;
import io.camunda.zeebe.db.impl.DbLong;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class DbTransactionTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final ZeebeDbFactory<ColumnFamilies> dbFactory =
      EzeZeebeDbFactory.getDefaultFactory();

  private TransactionContext transactionContext;

  private ColumnFamily<DbLong, DbLong> oneColumnFamily;
  private ColumnFamily<DbLong, DbLong> twoColumnFamily;
  private ColumnFamily<DbLong, DbLong> threeColumnFamily;

  private DbLong oneKey;
  private DbLong oneValue;
  private DbLong twoValue;
  private DbLong twoKey;
  private DbLong threeKey;
  private DbLong threeValue;

  @Before
  public void setup() throws Exception {
    final File pathName = temporaryFolder.newFolder();
    final ZeebeDb<ColumnFamilies> zeebeDb = dbFactory.createDb(pathName);
    transactionContext = zeebeDb.createContext();

    oneKey = new DbLong();
    oneValue = new DbLong();
    oneColumnFamily =
        zeebeDb.createColumnFamily(ColumnFamilies.ONE, transactionContext, oneKey, oneValue);

    twoKey = new DbLong();
    twoValue = new DbLong();
    twoColumnFamily =
        zeebeDb.createColumnFamily(ColumnFamilies.TWO, transactionContext, twoKey, twoValue);

    threeKey = new DbLong();
    threeValue = new DbLong();
    threeColumnFamily =
        zeebeDb.createColumnFamily(ColumnFamilies.THREE, transactionContext, threeKey, threeValue);
  }

  @Test
  public void shouldUseTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    // when
    transactionContext.runInTransaction(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          twoColumnFamily.put(twoKey, twoValue);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
  }

  @Test
  public void shouldNotGetPreviousValue() {
    // given
    oneKey.wrapLong(123);
    oneValue.wrapLong(456);

    transactionContext.runInTransaction(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          oneColumnFamily.get(oneKey);
          oneKey.wrapLong(-1);

          // when
          final DbLong zbLong = oneColumnFamily.get(oneKey);

          // then
          assertThat(zbLong).isNull();
        });
  }

  @Test
  public void shouldStartNewTransaction() throws Exception {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    final ZeebeDbTransaction transaction = transactionContext.getCurrentTransaction();
    transaction.run(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          twoColumnFamily.put(twoKey, twoValue);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // when
    transaction.commit();

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
  }

  @Test
  public void shouldAccessOnOpenTransaction() throws Exception {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    final ZeebeDbTransaction transaction = transactionContext.getCurrentTransaction();
    transaction.run(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          twoColumnFamily.put(twoKey, twoValue);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // when
    // no commit

    // then
    // uses the same transaction
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
  }

  @Test
  public void shouldNotReopenTransaction() throws Exception {
    // given
    final ZeebeDbTransaction transaction = transactionContext.getCurrentTransaction();

    transaction.run(
        () -> {

          // when
          final ZeebeDbTransaction sameTransaction = transactionContext.getCurrentTransaction();

          // then
          assertThat(transaction).isEqualTo(sameTransaction);
        });
  }

  @Test
  public void shouldNotReopenTransactionWithOperations() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    transactionContext.runInTransaction(
        () -> {

          // when
          final ZeebeDbTransaction sameTransaction = transactionContext.getCurrentTransaction();
          sameTransaction.run(
              () -> {
                oneColumnFamily.put(oneKey, oneValue);
                twoColumnFamily.put(twoKey, twoValue);
                threeColumnFamily.put(threeKey, threeValue);
              });
          sameTransaction.commit();

          // then it is committed but available in this transaction
          assertThat(oneColumnFamily.exists(oneKey)).isTrue();
          oneColumnFamily.delete(oneKey);

          assertThat(twoColumnFamily.exists(twoKey)).isTrue();
          assertThat(threeColumnFamily.exists(threeKey)).isTrue();
        });

    // then it is committed but available in this transaction
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
  }

  @Test
  public void shouldRollbackTransaction() throws Exception {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    final ZeebeDbTransaction transaction = transactionContext.getCurrentTransaction();
    transaction.run(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          twoColumnFamily.put(twoKey, twoValue);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // when
    transaction.rollback();

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    assertThat(twoColumnFamily.exists(twoKey)).isFalse();
    assertThat(threeColumnFamily.exists(threeKey)).isFalse();
  }

  @Test
  public void shouldGetValueInTransaction() {
    // given
    final AtomicLong actualValue = new AtomicLong(0);
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    // when
    transactionContext.runInTransaction(
        () -> {
          oneColumnFamily.put(oneKey, oneValue);
          final DbLong value = oneColumnFamily.get(oneKey);
          actualValue.set(value.getValue());
        });

    // then
    assertThat(actualValue.get()).isEqualTo(-1);
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(-1);
  }

  @Test
  public void shouldFindValueInTransaction() {
    // given
    final Map<Long, Long> actualValues = new HashMap<>();
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    transactionContext.runInTransaction(
        () -> {
          // update value
          oneKey.wrapLong(1);
          oneValue.wrapLong(-2);
          oneColumnFamily.put(oneKey, oneValue);

          // create new key-value pair
          oneKey.wrapLong(2);
          oneValue.wrapLong(-3);
          oneColumnFamily.put(oneKey, oneValue);

          actualValues.put(oneKey.getValue(), oneColumnFamily.get(oneKey).getValue());
          oneKey.wrapLong(1);
          actualValues.put(oneKey.getValue(), oneColumnFamily.get(oneKey).getValue());
        });

    // then
    final Map<Long, Long> expectedValues = new HashMap<>();
    expectedValues.put(1L, -2L);
    expectedValues.put(2L, -3L);
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  @Test
  public void shouldIterateAndFindValuesInTransaction() {
    // given
    final Map<Long, Long> actualValues = new HashMap<>();

    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    oneKey.wrapLong(2);
    oneValue.wrapLong(-2);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    transactionContext.runInTransaction(
        () -> {
          // update old value
          oneKey.wrapLong(2);
          oneValue.wrapLong(-5);
          oneColumnFamily.put(oneKey, oneValue);

          // create new key-value pair
          oneKey.wrapLong(3);
          oneValue.wrapLong(-3);
          oneColumnFamily.put(oneKey, oneValue);

          oneColumnFamily.forEach((k, v) -> actualValues.put(k.getValue(), v.getValue()));
        });

    // then
    final Map<Long, Long> expectedValues = new HashMap<>();
    expectedValues.put(1L, -1L);
    expectedValues.put(2L, -5L);
    expectedValues.put(3L, -3L);
    assertThat(actualValues).isEqualTo(expectedValues);
  }

  @Test
  public void shouldIterateAndDeleteInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    oneKey.wrapLong(2);
    oneValue.wrapLong(-2);
    oneColumnFamily.put(oneKey, oneValue);

    // when
    transactionContext.runInTransaction(
        () -> oneColumnFamily.forEach((k, v) -> oneColumnFamily.delete(k)));

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    oneKey.wrapLong(2);
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
  }

  @Test
  public void shouldEndInSameTransaction() {
    // given
    final AtomicLong actualValue = new AtomicLong(0);
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);
    oneColumnFamily.put(oneKey, oneValue);

    twoValue.wrapLong(192313);

    // when
    oneColumnFamily.put(oneKey, oneValue);
    transactionContext.runInTransaction(
        () -> {
          transactionContext.runInTransaction(() -> oneColumnFamily.put(oneKey, twoValue));
          final DbLong value = oneColumnFamily.get(oneKey);
          actualValue.set(value.getValue());
        });

    // then
    assertThat(actualValue.get()).isEqualTo(192313);
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(192313);
  }

  @Test
  public void shouldWriteAndDeleteInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);
    twoColumnFamily.put(twoKey, twoValue);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);
    threeColumnFamily.put(threeKey, threeValue);

    // when
    transactionContext.runInTransaction(
        () -> {
          // create
          oneColumnFamily.put(oneKey, oneValue);

          // delete
          twoColumnFamily.delete(twoKey);

          // update
          threeValue.wrapLong(Integer.MIN_VALUE);
          threeColumnFamily.put(threeKey, threeValue);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isTrue();
    assertThat(oneColumnFamily.get(oneKey).getValue()).isEqualTo(-1);

    assertThat(twoColumnFamily.exists(twoKey)).isFalse();

    assertThat(threeColumnFamily.exists(threeKey)).isTrue();
    assertThat(threeColumnFamily.get(threeKey).getValue()).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void shouldWriteAndDeleteSameKeyValuePairInTransaction() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    // when
    transactionContext.runInTransaction(
        () -> {
          // create
          oneColumnFamily.put(oneKey, oneValue);

          // delete
          oneColumnFamily.delete(oneKey);
        });

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
  }

  @Test
  public void shouldNotCommitOnError() {
    // given
    oneKey.wrapLong(1);
    oneValue.wrapLong(-1);

    twoKey.wrapLong(52000);
    twoValue.wrapLong(192313);
    twoColumnFamily.put(twoKey, twoValue);

    threeKey.wrapLong(Short.MAX_VALUE);
    threeValue.wrapLong(Integer.MAX_VALUE);

    // when
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    try {
      transactionContext.runInTransaction(
          () -> {
            oneColumnFamily.put(oneKey, oneValue);
            twoColumnFamily.delete(twoKey);
            threeColumnFamily.put(threeKey, threeValue);
            throw new RuntimeException();
          });
    } catch (final Exception e) {
      // ignore
    }

    // then
    assertThat(oneColumnFamily.exists(oneKey)).isFalse();
    assertThat(twoColumnFamily.exists(twoKey)).isTrue();
    assertThat(threeColumnFamily.exists(threeKey)).isFalse();
  }

  private enum ColumnFamilies {
    DEFAULT, // rocksDB needs a default column family
    ONE,
    TWO,
    THREE
  }
}
