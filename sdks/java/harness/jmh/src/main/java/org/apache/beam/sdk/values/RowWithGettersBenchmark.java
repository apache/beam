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
package org.apache.beam.sdk.values;

import static java.math.BigDecimal.ONE;
import static java.nio.charset.StandardCharsets.*;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils.random;
import static org.apache.beam.sdk.values.RowWithGettersBenchmark.MapOfPrimitiveBundle.primitiveMapPojo;
import static org.apache.beam.sdk.values.RowWithGettersBenchmark.SimplePojoBundle.simplePojo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.utils.TestPOJOs;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedArraysPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedCollectionPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedMapPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.PrimitiveArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.PrimitiveMapPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.SimplePOJO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks on reading field values from {@link RowWithGetters}.
 *
 * <ul>
 *   <li>The score doesn't reflect read access only, measurement includes iterating over a large
 *       number of rows. What matters are the relative changes.
 *   <li>You cannot easily compare scores between different benchmarks as rows contain different
 *       number of fields. Also, depending on types, fields are read recursively to measure the
 *       impact of lazy vs eager data structures.
 *   <li>Any cache initialization is done lazily on first read (if caching is enabled) to include
 *       associated costs in the measurement.
 * </ul>
 *
 * <p>Each benchmark method invocation processes a bundle of {@link #BUNDLE_SIZE} rows and
 * recursively reads all values per row. The rows are created upfront using JMH {@link State} to
 * exclude any initialization costs from the measurement. To prevent unintended cache hits in {@link
 * RowWithGetters} a new bundle of rows must be generated before every invocation.
 *
 * <p>Using state setup per {@link Level#Invocation} has significant drawbacks by itself! Though,
 * given that reading bundles of rows of size {@link #BUNDLE_SIZE} takes well above 1 ms, each
 * individual invocation can be adequately timestamped without risking generating wrong results.
 */
public class RowWithGettersBenchmark {
  public static final int BUNDLE_SIZE = 50000;
  public static final int COLLECTION_SIZE = 10;

  // Benchmark simple POJO (lots of fields!)
  @Benchmark
  public void readPojo(SimplePojoBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark nested POJO (nested one same as above)
  @Benchmark
  public void readNestedPojo(NestedPojoBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark nested POJO containing map of int values
  @Benchmark
  public void readNestedMapOfPrimitive(NestedMapOfPrimitiveBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing primitive arrays
  @Benchmark
  public void readArraysOfPrimitives(ArrayOfPrimitivesBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing single array of simple POJO
  @Benchmark
  public void readArrayOfPojo(ArrayOfPojoBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing single array of POJO containing map of int values
  @Benchmark
  public void readArrayOfPojoMapOfPrimitive(ArrayOfPojoMapOfPrimitiveBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing array of string array
  @Benchmark
  public void readArrayOfStringArray(ArrayOfStringArrayBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing collections of simple POJO
  @Benchmark
  public void readCollectionsOfPojo(CollectionsOfPojoBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing map of int values
  @Benchmark
  public void readMapOfPrimitive(MapOfPrimitiveBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // Benchmark POJO containing map of POJO values
  @Benchmark
  public void readMapOfPojo(MapOfPojoBundle state, Blackhole bh) {
    state.readFields(bh);
  }

  // States

  @State(Scope.Benchmark)
  public static class SimplePojoBundle extends Bundle<SimplePOJO> {
    static final DateTime DATE = DateTime.parse("1979-03-14");
    static final Instant INSTANT = DateTime.parse("1979-03-15").toInstant();

    public SimplePojoBundle() {
      super(SimplePOJO.class);
    }

    @Override
    SimplePOJO factory(int i) {
      return simplePojo(i);
    }

    static SimplePOJO simplePojo(int i) {
      return new SimplePOJO(
          str(10),
          (byte) 1,
          (short) 2,
          i,
          4L + i,
          true,
          DATE,
          INSTANT,
          bytes(10),
          byteBuffer(10),
          ONE,
          strBuilder(i));
    }
  }

  @State(Scope.Benchmark)
  public static class NestedPojoBundle extends Bundle<NestedPOJO> {
    public NestedPojoBundle() {
      super(NestedPOJO.class);
    }

    @Override
    NestedPOJO factory(int i) {
      return new NestedPOJO(simplePojo(i));
    }
  }

  @State(Scope.Benchmark)
  public static class ArrayOfPrimitivesBundle extends Bundle<PrimitiveArrayPOJO> {
    public ArrayOfPrimitivesBundle() {
      super(PrimitiveArrayPOJO.class);
    }

    @Override
    PrimitiveArrayPOJO factory(int i) {
      List<String> strings = ImmutableList.of("a", "b", "c", random(10));
      int[] integers = IntStream.range(0, COLLECTION_SIZE).toArray();
      Long[] longs = LongStream.range(0, COLLECTION_SIZE).boxed().toArray(Long[]::new);
      return new PrimitiveArrayPOJO(strings, integers, longs);
    }
  }

  @State(Scope.Benchmark)
  public static class NestedMapOfPrimitiveBundle extends Bundle<NestedPrimitiveMapPOJO> {
    public NestedMapOfPrimitiveBundle() {
      super(NestedPrimitiveMapPOJO.class);
    }

    @Override
    NestedPrimitiveMapPOJO factory(int i) {
      return new NestedPrimitiveMapPOJO(primitiveMapPojo(i));
    }
  }

  @State(Scope.Benchmark)
  public static class ArrayOfPojoBundle extends Bundle<NestedArrayPOJO> {
    public ArrayOfPojoBundle() {
      super(NestedArrayPOJO.class);
    }

    @Override
    NestedArrayPOJO factory(int i) {
      SimplePOJO[] pojos =
          IntStream.range(0, COLLECTION_SIZE)
              .mapToObj(SimplePojoBundle::simplePojo)
              .toArray(SimplePOJO[]::new);
      return new NestedArrayPOJO(pojos);
    }
  }

  @State(Scope.Benchmark)
  public static class ArrayOfPojoMapOfPrimitiveBundle extends Bundle<NestedArrayPrimitiveMapPOJO> {
    public ArrayOfPojoMapOfPrimitiveBundle() {
      super(NestedArrayPrimitiveMapPOJO.class);
    }

    @Override
    NestedArrayPrimitiveMapPOJO factory(int i) {
      PrimitiveMapPOJO[] pojos =
          IntStream.range(0, COLLECTION_SIZE)
              .mapToObj(MapOfPrimitiveBundle::primitiveMapPojo)
              .toArray(PrimitiveMapPOJO[]::new);
      return new NestedArrayPrimitiveMapPOJO(pojos);
    }
  }

  @State(Scope.Benchmark)
  public static class ArrayOfStringArrayBundle extends Bundle<NestedArraysPOJO> {
    public ArrayOfStringArrayBundle() {
      super(NestedArraysPOJO.class);
    }

    @Override
    NestedArraysPOJO factory(int i) {
      List<String> pojos =
          range(0, COLLECTION_SIZE * COLLECTION_SIZE).mapToObj(Integer::toString).collect(toList());
      return new NestedArraysPOJO(Lists.partition(pojos, COLLECTION_SIZE));
    }
  }

  @State(Scope.Benchmark)
  public static class CollectionsOfPojoBundle extends Bundle<NestedCollectionPOJO> {
    public CollectionsOfPojoBundle() {
      super(NestedCollectionPOJO.class);
    }

    @Override
    NestedCollectionPOJO factory(int i) {
      List<SimplePOJO> pojos =
          IntStream.range(0, COLLECTION_SIZE)
              .mapToObj(SimplePojoBundle::simplePojo)
              .collect(toList());
      return new NestedCollectionPOJO(pojos);
    }
  }

  @State(Scope.Benchmark)
  public static class MapOfPrimitiveBundle extends Bundle<PrimitiveMapPOJO> {
    public MapOfPrimitiveBundle() {
      super(PrimitiveMapPOJO.class);
    }

    @Override
    PrimitiveMapPOJO factory(int i) {
      return primitiveMapPojo(i);
    }

    static PrimitiveMapPOJO primitiveMapPojo(int i) {
      Map<String, Integer> map =
          IntStream.range(0, COLLECTION_SIZE)
              .boxed()
              .collect(toMap(key -> key.toString(), identity()));

      return new PrimitiveMapPOJO(map);
    }
  }

  @State(Scope.Benchmark)
  public static class MapOfPojoBundle extends Bundle<NestedMapPOJO> {
    public MapOfPojoBundle() {
      super(NestedMapPOJO.class);
    }

    @Override
    NestedMapPOJO factory(int i) {
      Map<String, SimplePOJO> map =
          IntStream.range(0, COLLECTION_SIZE)
              .boxed()
              .collect(toMap(key -> key.toString(), SimplePojoBundle::simplePojo));

      return new NestedMapPOJO(map);
    }
  }

  @State(Scope.Benchmark)
  public static class Bundle<T> {
    private final SchemaRegistry registry;
    private final Class<T> clazz;

    private SerializableFunction<T, Row> toRow;
    private List<Row> rows;

    @Param({"1", "3"})
    int reads;

    @Param({"false", "HashMap", "TreeMap"})
    String cache;

    @Param({"false", "true"})
    boolean persistPojoLists;

    // ignore
    public Bundle() { // just to silence warnings
      this(null);
    }

    public Bundle(Class<T> clazz) {
      this.clazz = clazz;
      this.registry = SchemaRegistry.createDefault();
    }

    T factory(int i) {
      return null;
    }

    // Level must be Invocation to not generate cache hits by repeatedly reading the same rows.
    @Setup(Level.Invocation)
    public void setup() throws NoSuchSchemaException {
      GetterBasedSchemaProvider.persistPojoLists = persistPojoLists;
      if (cache.equals("HashMap")) {
        RowWithGetters.cacheSupplier = HashMap::new;
      } else if (cache.equals("TreeMap")) {
        RowWithGetters.cacheSupplier = TreeMap::new;
      } else {
        RowWithGetters.cacheSupplier = null;
      }

      if (toRow == null) {
        toRow = registry.getToRowFunction(clazz);
      }
      // prepare bundle of rows to exclude associated costs from benchmark
      rows =
          range(0, BUNDLE_SIZE)
              .mapToObj(i -> toRow.apply(factory(i)))
              .collect(toCollection(() -> new ArrayList<>(BUNDLE_SIZE)));
    }

    void readFields(Blackhole blackhole) {
      for (Row row : rows) {
        for (int i = 0; i < reads; i++) {
          recursiveReadRow(row, blackhole);
        }
      }
    }

    private void recursiveReadRow(Row row, Blackhole blackhole) {
      for (int idx = 0; idx < row.getFieldCount(); idx++) {
        FieldType fieldType = row.getSchema().getField(idx).getType();
        TypeName typeName = fieldType.getTypeName();
        if (TypeName.COLLECTION_TYPES.contains(typeName)) {
          if (fieldType.getCollectionElementType().getTypeName().equals(TypeName.ROW)) {
            row.getIterable(idx).forEach(e -> recursiveReadRow((Row) e, blackhole));
          } else {
            row.getIterable(idx).forEach(blackhole::consume);
          }
        } else if (typeName.equals(TypeName.MAP)) {
          if (fieldType.getMapValueType().getTypeName().equals(TypeName.ROW)) {
            row.getMap(idx)
                .entrySet()
                .forEach(e -> recursiveReadRow((Row) e.getValue(), blackhole));
          } else {
            row.getMap(idx).entrySet().forEach(blackhole::consume);
          }
        } else if (typeName.equals(TypeName.ROW)) {
          recursiveReadRow(row.getRow(idx), blackhole);
        } else {
          blackhole.consume(row.getValue(idx));
        }
      }
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedPrimitiveMapPOJO {
    public TestPOJOs.PrimitiveMapPOJO nested;

    public NestedPrimitiveMapPOJO(TestPOJOs.PrimitiveMapPOJO nested) {
      this.nested = nested;
    }

    public NestedPrimitiveMapPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        NestedPrimitiveMapPOJO that = (NestedPrimitiveMapPOJO) o;
        return Objects.equals(this.nested, that.nested);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(new Object[] {this.nested});
    }
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class NestedArrayPrimitiveMapPOJO {
    public TestPOJOs.PrimitiveMapPOJO[] pojos;

    public NestedArrayPrimitiveMapPOJO(TestPOJOs.PrimitiveMapPOJO... pojos) {
      this.pojos = pojos;
    }

    public NestedArrayPrimitiveMapPOJO() {}

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        NestedArrayPrimitiveMapPOJO that = (NestedArrayPrimitiveMapPOJO) o;
        return Arrays.equals(this.pojos, that.pojos);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(this.pojos);
    }
  }

  private static String str(int size) {
    return random(size);
  }

  private static StringBuilder strBuilder(int i) {
    return new StringBuilder("builder").append(i);
  }

  private static byte[] bytes(int size) {
    return str(size).getBytes(UTF_8);
  }

  private static ByteBuffer byteBuffer(int size) {
    return ByteBuffer.wrap(bytes(size));
  }
}
