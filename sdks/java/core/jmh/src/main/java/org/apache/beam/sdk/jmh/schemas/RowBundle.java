/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.jmh.schemas;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Bundle of rows according to the configured {@link Factory} as input for benchmarks.
 *
 * <p>When reading, rows are created during {@link #setup()} to exclude initialization costs from
 * the measurement. To prevent unintended cache hits in {@link RowWithGetters}, a new bundle of rows
 * must be generated before every invocation.
 *
 * <p>Setup per {@link Level#Invocation} has considerable drawbacks. Though, given that processing
 * bundles of rows (n={@link #bundleSize}) takes well above 1 ms, each individual invocation can be
 * adequately timestamped without risking generating wrong results.
 */
@State(Scope.Benchmark)
public class RowBundle<T> {
  public enum Action {
    /**
     * Write field to object using {@link
     * GetterBasedSchemaProvider#fromRowFunction(TypeDescriptor)}.
     *
     * <p>Use {@link RowWithStorage} to bypass optimizations in RowWithGetters for writes.
     */
    WRITE,

    /**
     * Read field from {@link RowWithGetters} provided by {@link
     * GetterBasedSchemaProvider#toRowFunction(TypeDescriptor)}.
     */
    READ_ONCE,

    /**
     * Repeatedly (3x) read field from {@link RowWithGetters} provided by {@link
     * GetterBasedSchemaProvider#toRowFunction(TypeDescriptor)}.
     */
    READ_REPEATED
  }

  private static final SchemaRegistry REGISTRY = SchemaRegistry.createDefault();

  private final SerializableFunction<Row, T> fromRow;
  private final SerializableFunction<T, Row> toRow;

  private final Row rowWithStorage;

  private final T rowTarget;

  private Row[] rows;

  @Param("1000000")
  int bundleSize;

  @Param({"READ_ONCE", "READ_REPEATED", "WRITE"})
  Action action;

  public RowBundle() {
    this(null); // unused, just to prevent warnings
  }

  public RowBundle(Class<T> clazz) {
    try {
      SchemaCoder<T> coder = REGISTRY.getSchemaCoder(clazz);
      if (coder.getSchema().getFieldCount() != 1) {
        throw new IllegalArgumentException("Expected class with a single field");
      }
      fromRow = coder.getFromRowFunction();
      toRow = coder.getToRowFunction();
      rowWithStorage = createRowWithStorage(coder.getSchema());
      rowTarget = fromRow.apply(rowWithStorage);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  @Setup(Level.Invocation)
  public void setup() {
    // no mutable state in case of writes, skip setup
    if (action == Action.WRITE) {
      return;
    }
    if (rows == null) {
      rows = new Row[bundleSize];
    }
    // new rows (with getters) for each invocation to prevent accidental cache hits
    for (int i = 0; i < bundleSize; i++) {
      rows[i] = toRow.apply(rowTarget);
    }
  }

  /** Runs benchmark iteration on a bundle of rows. */
  public void processRows(Blackhole blackhole) {
    if (action == Action.READ_ONCE) {
      readRowsOnce(blackhole);
    } else if (action == Action.READ_REPEATED) {
      readRowsRepeatedly(blackhole);
    } else {
      writeRows(blackhole);
    }
  }

  /** Reads single field from row (of type {@link RowWithGetters}). */
  protected void readField(Row row, Blackhole blackhole) {
    blackhole.consume(row.getValue(0));
  }

  private void readRowsOnce(Blackhole blackhole) {
    for (Row row : rows) {
      readField(row, blackhole);
    }
  }

  private void readRowsRepeatedly(Blackhole blackhole) {
    for (Row row : rows) {
      readField(row, blackhole);
      readField(row, blackhole);
      readField(row, blackhole);
    }
  }

  private void writeRows(Blackhole blackhole) {
    for (int i = 0; i < bundleSize; i++) {
      blackhole.consume(fromRow.apply(rowWithStorage));
    }
  }

  private static final Instant TODAY = DateTime.now().withTimeAtStartOfDay().toInstant();

  /** Creates row of type {@link RowWithStorage} with single field matching the provided schema. */
  private static Row createRowWithStorage(Schema schema) {
    return RowWithStorage.withSchema(schema)
        .attachValues(createValue(42, schema.getField(0).getType()));
  }

  private static Object createValue(int val, FieldType type) {
    switch (type.getTypeName()) {
      case STRING:
        return String.valueOf(val);
      case INT32:
        return val;
      case BYTES:
        return String.valueOf(val).getBytes(StandardCharsets.UTF_8);
      case DATETIME:
        return TODAY.minus(Duration.standardHours(val));
      case ROW:
        return createRowWithStorage(type.getRowSchema());
      case ARRAY:
      case ITERABLE:
        return ImmutableList.of(createValue(val, type.getCollectionElementType()));
      case MAP:
        return ImmutableMap.of(
            createValue(val, type.getMapKeyType()), createValue(val, type.getMapValueType()));
      default:
        throw new RuntimeException("No value factory for type " + type);
    }
  }
}
