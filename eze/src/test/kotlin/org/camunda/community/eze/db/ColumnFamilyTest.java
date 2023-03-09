/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package org.camunda.community.eze.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.ZeebeDbFactory;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DefaultColumnFamily;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class ColumnFamilyTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
      EzeZeebeDbFactory.getDefaultFactory();
  private ZeebeDb<DefaultColumnFamily> zeebeDb;
  private ColumnFamily<DbLong, DbLong> columnFamily;
  private DbLong key;
  private DbLong value;

  @Before
  public void setup() throws Exception {
    final File pathName = temporaryFolder.newFolder();
    zeebeDb = dbFactory.createDb(pathName);

    key = new DbLong();
    value = new DbLong();
    columnFamily =
        zeebeDb.createColumnFamily(
            DefaultColumnFamily.DEFAULT, zeebeDb.createContext(), key, value);
  }

  @Test
  public void shouldInsertValue() {
    // given
    key.wrapLong(1213);
    value.wrapLong(255);

    // when
    columnFamily.insert(key, value);
    value.wrapLong(221);

    // then
    final DbLong zbLong = columnFamily.get(key);

    assertThat(zbLong).isNotNull();
    assertThat(zbLong.getValue()).isEqualTo(255);

    // zbLong and value are referencing the same object
    assertThat(value.getValue()).isEqualTo(255);
  }

  @Test
  public void shouldNotInsertIfExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    columnFamily.insert(key, value);

    // when/then
    assertThatThrownBy(() -> columnFamily.insert(key, value))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shouldUpdateValue() {
    // given
    key.wrapLong(1213);
    value.wrapLong(255);
    columnFamily.insert(key, value);

    // when
    value.wrapLong(256);
    columnFamily.update(key, value);

    // then
    final DbLong zbLong = columnFamily.get(key);

    assertThat(zbLong).isNotNull();
    assertThat(zbLong.getValue()).isEqualTo(256);
  }

  @Test
  public void shouldNotUpdateIfNotExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    // when/then
    assertThatThrownBy(() -> columnFamily.update(key, value))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shouldUpsertIfExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    columnFamily.insert(key, value);

    // when
    value.wrapLong(11);
    columnFamily.upsert(key, value);

    // then
    final var persistedValue = columnFamily.get(key);

    assertThat(persistedValue.getValue()).isEqualTo(11);
  }

  @Test
  public void shouldUpsertIfNotExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    // when
    columnFamily.upsert(key, value);

    // then
    final var persistedValue = columnFamily.get(key);

    assertThat(persistedValue.getValue()).isEqualTo(10);
  }

  @Test
  public void shouldReturnNullIfNotExist() {
    // given
    key.wrapLong(1213);

    // when
    final DbLong zbLong = columnFamily.get(key);

    // then
    assertThat(zbLong).isNull();
  }

  @Test
  public void shouldPutMultipleValues() {
    // given
    putKeyValuePair(1213, 255);

    // when
    putKeyValuePair(456789, 12345);
    value.wrapLong(221);

    // then
    key.wrapLong(1213);
    DbLong longValue = columnFamily.get(key);

    assertThat(longValue).isNotNull();
    assertThat(longValue.getValue()).isEqualTo(255);

    key.wrapLong(456789);
    longValue = columnFamily.get(key);

    assertThat(longValue).isNotNull();
    assertThat(longValue.getValue()).isEqualTo(12345);
  }

  @Test
  public void shouldPutAndGetMultipleValues() {
    // given
    putKeyValuePair(1213, 255);

    // when
    DbLong longValue = columnFamily.get(key);
    putKeyValuePair(456789, 12345);
    value.wrapLong(221);

    // then
    assertThat(longValue.getValue()).isEqualTo(221);
    key.wrapLong(1213);
    longValue = columnFamily.get(key);

    assertThat(longValue).isNotNull();
    assertThat(longValue.getValue()).isEqualTo(255);

    key.wrapLong(456789);
    longValue = columnFamily.get(key);

    assertThat(longValue).isNotNull();
    assertThat(longValue.getValue()).isEqualTo(12345);
  }

  @Test
  public void shouldCheckForExistence() {
    // given
    putKeyValuePair(1213, 255);

    // when
    final boolean exists = columnFamily.exists(key);

    // then
    assertThat(exists).isTrue();
  }

  @Test
  public void shouldNotExist() {
    // given
    key.wrapLong(1213);

    // when
    final boolean exists = columnFamily.exists(key);

    // then
    assertThat(exists).isFalse();
  }

  @Test
  public void shouldDeleteExisting() {
    // given
    putKeyValuePair(1213, 255);

    // when
    columnFamily.deleteExisting(key);

    // then
    final boolean exists = columnFamily.exists(key);
    assertThat(exists).isFalse();

    final DbLong zbLong = columnFamily.get(key);
    assertThat(zbLong).isNull();
  }

  @Test
  public void shouldNotDeleteExistingIfNotExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    // when/then
    assertThatThrownBy(() -> columnFamily.deleteExisting(key))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void shouldDeleteIfExists() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    columnFamily.insert(key, value);

    // when
    columnFamily.deleteIfExists(key);

    // then
    final var persistedValue = columnFamily.get(key);

    assertThat(persistedValue).isNull();
  }

  @Test
  public void shouldIgnoreDeleteIfNotExist() {
    // given
    key.wrapLong(1);
    value.wrapLong(10);

    // when/then
    assertThatCode(() -> columnFamily.deleteIfExists(key)).doesNotThrowAnyException();
  }

  @Test
  public void shouldNotDeleteDifferentKey() {
    // given
    putKeyValuePair(1213, 255);

    // when
    key.wrapLong(700);
    columnFamily.deleteIfExists(key);

    // then
    key.wrapLong(1213);
    final boolean exists = columnFamily.exists(key);
    assertThat(exists).isTrue();

    final DbLong zbLong = columnFamily.get(key);
    assertThat(zbLong).isNotNull();
    assertThat(zbLong.getValue()).isEqualTo(255);
  }

  @Test
  public void shouldUseForeachValue() {
    // given
    putKeyValuePair(4567, 123);
    putKeyValuePair(6734, 921);
    putKeyValuePair(1213, 255);
    putKeyValuePair(1, Short.MAX_VALUE);
    putKeyValuePair(Short.MAX_VALUE, 1);

    // when
    final List<Long> values = new ArrayList<>();
    columnFamily.forEach((value) -> values.add(value.getValue()));

    // then
    assertThat(values).containsExactly((long) Short.MAX_VALUE, 255L, 123L, 921L, 1L);
  }

  @Test
  public void shouldUseForeachPair() {
    // given
    putKeyValuePair(4567, 123);
    putKeyValuePair(6734, 921);
    putKeyValuePair(1213, 255);
    putKeyValuePair(1, Short.MAX_VALUE);
    putKeyValuePair(Short.MAX_VALUE, 1);

    // when
    final List<Long> keys = new ArrayList<>();
    final List<Long> values = new ArrayList<>();
    columnFamily.forEach(
        (key, value) -> {
          keys.add(key.getValue());
          values.add(value.getValue());
        });

    // then
    assertThat(keys).containsExactly(1L, 1213L, 4567L, 6734L, (long) Short.MAX_VALUE);
    assertThat(values).containsExactly((long) Short.MAX_VALUE, 255L, 123L, 921L, 1L);
  }

  @Test
  public void shouldDeleteOnForeachPair() {
    // given
    putKeyValuePair(4567, 123);
    putKeyValuePair(6734, 921);
    putKeyValuePair(1213, 255);
    putKeyValuePair(1, Short.MAX_VALUE);
    putKeyValuePair(Short.MAX_VALUE, 1);

    // when
    columnFamily.forEach(
        (key, value) -> {
          columnFamily.deleteIfExists(key);
        });

    final List<Long> keys = new ArrayList<>();
    final List<Long> values = new ArrayList<>();
    columnFamily.forEach(
        (key, value) -> {
          keys.add(key.getValue());
          values.add(value.getValue());
        });

    // then
    assertThat(keys).isEmpty();
    assertThat(values).isEmpty();
    key.wrapLong(4567L);
    assertThat(columnFamily.exists(key)).isFalse();

    key.wrapLong(6734);
    assertThat(columnFamily.exists(key)).isFalse();

    key.wrapLong(1213);
    assertThat(columnFamily.exists(key)).isFalse();

    key.wrapLong(1);
    assertThat(columnFamily.exists(key)).isFalse();

    key.wrapLong(Short.MAX_VALUE);
    assertThat(columnFamily.exists(key)).isFalse();
  }

  @Test
  public void shouldUseWhileTrue() {
    // given
    putKeyValuePair(4567, 123);
    putKeyValuePair(6734, 921);
    putKeyValuePair(1213, 255);
    putKeyValuePair(1, Short.MAX_VALUE);
    putKeyValuePair(Short.MAX_VALUE, 1);

    // when
    final List<Long> keys = new ArrayList<>();
    final List<Long> values = new ArrayList<>();
    columnFamily.whileTrue(
        (key, value) -> {
          keys.add(key.getValue());
          values.add(value.getValue());

          return key.getValue() != 4567;
        });

    // then
    assertThat(keys).containsExactly(1L, 1213L, 4567L);
    assertThat(values).containsExactly((long) Short.MAX_VALUE, 255L, 123L);
  }

  @Test
  public void shouldDeleteWhileTrue() {
    // given
    putKeyValuePair(4567, 123);
    putKeyValuePair(6734, 921);
    putKeyValuePair(1213, 255);
    putKeyValuePair(1, Short.MAX_VALUE);
    putKeyValuePair(Short.MAX_VALUE, 1);

    // when
    columnFamily.whileTrue(
        (key, value) -> {
          columnFamily.deleteIfExists(key);
          return key.getValue() != 4567;
        });

    final List<Long> keys = new ArrayList<>();
    final List<Long> values = new ArrayList<>();
    columnFamily.forEach(
        (key, value) -> {
          keys.add(key.getValue());
          values.add(value.getValue());
        });

    // then
    assertThat(keys).containsExactly(6734L, (long) Short.MAX_VALUE);
    assertThat(values).containsExactly(921L, 1L);
  }

  @Test
  public void shouldCheckIfEmpty() {
    assertThat(columnFamily.isEmpty()).isTrue();

    putKeyValuePair(1, 10);
    assertThat(columnFamily.isEmpty()).isFalse();

    columnFamily.deleteIfExists(key);
    assertThat(columnFamily.isEmpty()).isTrue();
  }

  @Test
  public void shouldUseWhileTrueWithStartKey() {
    // given
    putKeyValuePair(1, 10);
    putKeyValuePair(2, 20); // from here ---
    putKeyValuePair(3, 30);
    putKeyValuePair(4, 40); // to here   ---
    putKeyValuePair(5, 50);

    // when
    key.wrapLong(2);

    final List<Long> keys = new ArrayList<>();
    final List<Long> values = new ArrayList<>();
    columnFamily.whileTrue(
        key,
        (key, value) -> {
          keys.add(key.getValue());
          values.add(value.getValue());

          return key.getValue() != 4;
        });

    // then
    assertThat(keys).containsExactly(2L, 3L, 4L);
    assertThat(values).containsExactly(20L, 30L, 40L);
  }

  private void putKeyValuePair(final int key, final int value) {
    this.key.wrapLong(key);
    this.value.wrapLong(value);
    columnFamily.upsert(this.key, this.value);
  }
}
