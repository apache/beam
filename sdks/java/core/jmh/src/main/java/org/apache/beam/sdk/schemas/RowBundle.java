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
package org.apache.beam.sdk.schemas;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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
 * <p>The rows are created during {@link #setup()} to exclude initialization costs from the
 * measurement. To prevent unintended cache hits in {@link RowWithGetters}, a new bundle of rows
 * must be generated before every invocation.
 *
 * <p>Setup per {@link Level#Invocation} has considerable drawbacks. Though, given that processing
 * bundles of rows (n={@link #bundleSize}) takes well above 1 ms, each individual invocation can be
 * adequately timestamped without risking generating wrong results.
 */
@State(Scope.Benchmark)
public class RowBundle {
  @SuppressWarnings("ImmutableEnumChecker") // false positive
  public enum Action {
    /**
     * Write field to object using {@link
     * GetterBasedSchemaProvider#fromRowFunction(TypeDescriptor)}.
     *
     * <p>Use {@link RowWithStorage} to bypass optimizations in RowWithGetters for writes.
     */
    WRITE(Factory::createWithStorage),

    /**
     * Read field from {@link RowWithGetters} provided by {@link
     * GetterBasedSchemaProvider#toRowFunction(TypeDescriptor)}.
     */
    READ_ONCE(Factory::createWithGetter),

    /**
     * Repeatedly (3x) read field from {@link RowWithGetters} provided by {@link
     * GetterBasedSchemaProvider#toRowFunction(TypeDescriptor)}.
     */
    READ_REPEATED(Factory::createWithGetter);

    final Function<SchemaCoder<?>, Factory<Row>> factoryProvider;

    Action(Function<SchemaCoder<?>, Factory<Row>> factoryProvider) {
      this.factoryProvider = factoryProvider;
    }
  }

  private static final SchemaRegistry REGISTRY = SchemaRegistry.createDefault();

  private final SchemaCoder<?> coder;
  private final Sink<Row> sink;
  private Factory<Row> factory;

  protected Row[] rows;

  @Param("100000")
  int bundleSize;

  @Param({"READ_REPEATED", "WRITE"})
  Action action;

  public RowBundle() {
    this(null); // unused, just to prevent warnings
  }

  public RowBundle(Class<?> clazz) {
    try {
      coder = REGISTRY.getSchemaCoder(clazz);
      if (coder.getSchema().getFieldCount() != 1) {
        throw new IllegalArgumentException("Expected class with a single field");
      }
      sink = Sink.create(coder.getSchema());
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  @Setup(Level.Invocation)
  public void setup() {
    if (factory == null) {
      factory = action.factoryProvider.apply(coder);
      rows = new Row[bundleSize];
    }
    for (int i = 0; i < bundleSize; i++) {
      rows[i] = factory.apply(i);
    }
  }

  public void processRows(Blackhole blackhole) {
    if (action == Action.READ_ONCE) {
      readRowsOnce(blackhole);
    } else if (action == Action.READ_REPEATED) {
      readRowsRepeatedly(blackhole);
    } else {
      writeField(blackhole);
    }
  }

  private void readRowsOnce(Blackhole blackhole) {
    for (Row row : rows) {
      sink.accept(row, blackhole);
    }
  }

  private void readRowsRepeatedly(Blackhole blackhole) {
    for (Row row : rows) {
      sink.accept(row, blackhole);
      sink.accept(row, blackhole);
      sink.accept(row, blackhole);
    }
  }

  private void writeField(Blackhole blackhole) {
    SerializableFunction<Row, ?> fromRow = coder.getFromRowFunction();
    for (Row row : rows) {
      blackhole.consume(fromRow.apply(row));
    }
  }

  public interface Factory<T> extends Function<Integer, T> {
    Instant TODAY = DateTime.now().withTimeAtStartOfDay().toInstant();

    /** Create factory of rows of type {@link RowWithStorage}. */
    static Factory<Row> createWithStorage(Schema schema) {
      Factory<Object> fn = value(schema.getField(0).getType());
      return i -> RowWithStorage.withSchema(schema).attachValues(fn.apply(i));
    }

    /** Create factory of rows of type {@link RowWithStorage}. */
    static Factory<Row> createWithStorage(SchemaCoder<?> coder) {
      return createWithStorage(coder.getSchema());
    }

    /** Create factory of rows of type {@link RowWithGetters} by means of a {@link SchemaCoder}. */
    static <T> Factory<Row> createWithGetter(SchemaCoder<T> coder) {
      SerializableFunction<Row, T> fromRow = coder.getFromRowFunction();
      SerializableFunction<T, Row> toRow = coder.getToRowFunction();
      Factory<Row> factory = createWithStorage(coder.getSchema());
      // Factory creating Row -> Pojo -> RowWithGetters
      return i -> toRow.apply(fromRow.apply(factory.apply(i)));
    }

    static Factory<Object> value(Schema.FieldType type) {
      switch (type.getTypeName()) {
        case STRING:
          return i -> String.valueOf(i);
        case INT32:
          return i -> i;
        case BYTES:
          return i -> String.valueOf(i).getBytes(StandardCharsets.UTF_8);
        case DATETIME:
          return i -> TODAY.minus(Duration.standardHours(i));
        case ROW:
          return createWithStorage(type.getRowSchema())::apply;
        case ARRAY:
        case ITERABLE:
          return list(value(type.getCollectionElementType()));
        case MAP:
          return map(value(type.getMapKeyType()), value(type.getMapValueType()));
        default:
          throw new RuntimeException("No value factory for type " + type);
      }
    }

    static Factory<Object> list(Factory<Object> fn) {
      return i -> ImmutableList.of(fn.apply(i));
    }

    static Factory<Object> map(Factory<Object> kFn, Factory<Object> vFn) {
      return i -> ImmutableMap.of(kFn.apply(i), vFn.apply(i));
    }
  }

  interface Sink<T> extends BiConsumer<T, Blackhole> {
    Sink<Object> VALUE_SINK = (val, bh) -> bh.consume(val);

    /** Create sink for {@link Schema}, traversing recursive structures and collection types. */
    static Sink<Row> create(Schema schema) {
      Sink<Object> sink = sink(schema.getField(0).getType());
      return (row, bh) -> sink.accept(row.getValue(0), bh);
    }

    @SuppressWarnings("rawtypes")
    static Sink<Object> sink(Schema.FieldType type) {
      switch (type.getTypeName()) {
        case ARRAY:
          {
            Sink<Object> elemSink = sink(type.getCollectionElementType());
            return (list, bh) -> elemSink.accept(((List) list).get(0), bh);
          }
        case ITERABLE:
          {
            Sink<Object> elemSink = sink(type.getCollectionElementType());
            return (it, bh) -> elemSink.accept(Iterables.get((Iterable) it, 0), bh);
          }
        case MAP:
          {
            Sink<Entry> entrySink = entry(sink(type.getMapKeyType()), sink(type.getMapValueType()));
            return (map, bh) ->
                entrySink.accept(Iterables.get(((Map<Object, Object>) map).entrySet(), 0), bh);
          }
        case ROW:
          return (Sink) create(type.getRowSchema());
        default:
          return VALUE_SINK;
      }
    }

    @SuppressWarnings("rawtypes")
    static Sink<Entry> entry(Sink<Object> kSink, Sink<Object> vSink) {
      return (kv, bh) -> {
        kSink.accept(kv.getKey(), bh);
        vSink.accept(kv.getValue(), bh);
      };
    }
  }
}
