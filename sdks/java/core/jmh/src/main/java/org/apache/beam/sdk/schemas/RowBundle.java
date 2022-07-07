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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.sdk.values.TypeDescriptor;
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

  @Param({"READ_ONCE", "WRITE"})
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
    }
    rows = range(0, bundleSize).mapToObj(factory::apply).toArray(Row[]::new);
  }

  public void processRows(Blackhole blackhole) {
    if (action == Action.READ_ONCE) {
      readField(blackhole, 1);
    } else if (action == Action.READ_REPEATED) {
      readField(blackhole, 3);
    } else {
      writeField(blackhole);
    }
  }

  private void readField(Blackhole blackhole, int reads) {
    for (Row row : rows) {
      for (int i = 0; i < reads; i++) {
        sink.accept(row, blackhole);
      }
    }
  }

  private void writeField(Blackhole blackhole) {
    SerializableFunction<Row, ?> fromRow = coder.getFromRowFunction();
    for (Row row : rows) {
      blackhole.consume(fromRow.apply(row));
    }
  }

  public interface Factory<T> extends Function<Integer, T> {
    int ELEMENTS = 10;
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
      return i -> IntStream.range(i, ELEMENTS + i).mapToObj(fn::apply).collect(toList());
    }

    static Factory<Object> map(Factory<Object> kFn, Factory<Object> vFn) {
      return i -> IntStream.range(i, ELEMENTS + i).boxed().collect(toMap(kFn::apply, vFn::apply));
    }
  }

  interface Sink<T> extends BiConsumer<T, Blackhole> {

    /** Create sink for {@link Schema}, traversing recursive structures and collection types. */
    static Sink<Row> create(Schema schema) {
      Sink<Object> sink = sink(schema.getField(0).getType());
      return (row, bh) -> sink.accept(row.getValue(0), bh);
    }

    @SuppressWarnings("rawtypes")
    static Sink<Object> sink(Schema.FieldType type) {
      switch (type.getTypeName()) {
        case ITERABLE:
        case ARRAY:
          return foreach((Iterable it) -> it, sink(type.getCollectionElementType()));
        case MAP:
          Sink<Map.Entry> sink = entry(sink(type.getMapKeyType()), sink(type.getMapValueType()));
          return foreach((Map m) -> m.entrySet(), sink);
        case ROW:
          return (Sink) create(type.getRowSchema());
        default:
          return (val, bh) -> bh.consume(val);
      }
    }

    static <T, V> Sink<Object> foreach(Function<T, Iterable<V>> fn, Sink<V> sink) {
      return (val, bh) -> fn.apply((T) val).forEach(v -> sink.accept(v, bh));
    }

    @SuppressWarnings("rawtypes")
    static Sink<Map.Entry> entry(Sink<Object> kSink, Sink<Object> vSink) {
      return (kv, bh) -> {
        kSink.accept(kv.getKey(), bh);
        vSink.accept(kv.getValue(), bh);
      };
    }
  }
}
