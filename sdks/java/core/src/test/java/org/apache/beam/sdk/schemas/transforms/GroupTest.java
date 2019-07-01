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
package org.apache.beam.sdk.schemas.transforms;

import static org.apache.beam.sdk.TestUtils.KvMatcher.isKv;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link Group}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class GroupTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** A simple POJO for testing. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO implements Serializable {
    public String field1;
    public long field2;
    public String field3;

    public POJO(String field1, long field2, String field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    public POJO() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO pojo = (POJO) o;
      return field2 == pojo.field2
          && Objects.equals(field1, pojo.field1)
          && Objects.equals(field3, pojo.field3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }

    @Override
    public String toString() {
      return "POJO{"
          + "field1='"
          + field1
          + '\''
          + ", field2="
          + field2
          + ", field3='"
          + field3
          + '\''
          + '}';
    }
  }

  private static final Schema POJO_SCHEMA =
      Schema.builder()
          .addStringField("field1")
          .addInt64Field("field2")
          .addStringField("field3")
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByOneField() {
    PCollection<KV<Row, Iterable<POJO>>> grouped =
        pipeline
            .apply(
                Create.of(
                    new POJO("key1", 1, "value1"),
                    new POJO("key1", 2, "value2"),
                    new POJO("key2", 3, "value3"),
                    new POJO("key2", 4, "value4")))
            .apply(Group.byFieldNames("field1"));

    Schema keySchema = Schema.builder().addStringField("field1").build();
    List<KV<Row, Collection<POJO>>> expected =
        ImmutableList.of(
            KV.of(
                Row.withSchema(keySchema).addValue("key1").build(),
                ImmutableList.of(new POJO("key1", 1L, "value1"), new POJO("key1", 2L, "value2"))),
            KV.of(
                Row.withSchema(keySchema).addValue("key2").build(),
                ImmutableList.of(new POJO("key2", 3L, "value3"), new POJO("key2", 4L, "value4"))));

    PAssert.that(grouped).satisfies(actual -> containsKIterableVs(expected, actual, new POJO[0]));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByMultiple() {
    PCollection<KV<Row, Iterable<POJO>>> grouped =
        pipeline
            .apply(
                Create.of(
                    new POJO("key1", 1, "value1"),
                    new POJO("key1", 1, "value2"),
                    new POJO("key2", 2, "value3"),
                    new POJO("key2", 2, "value4")))
            .apply(Group.byFieldNames("field1", "field2"));

    Schema keySchema = Schema.builder().addStringField("field1").addInt64Field("field2").build();
    List<KV<Row, Collection<POJO>>> expected =
        ImmutableList.of(
            KV.of(
                Row.withSchema(keySchema).addValues("key1", 1L).build(),
                ImmutableList.of(new POJO("key1", 1L, "value1"), new POJO("key1", 1L, "value2"))),
            KV.of(
                Row.withSchema(keySchema).addValues("key2", 2L).build(),
                ImmutableList.of(new POJO("key2", 2L, "value3"), new POJO("key2", 2L, "value4"))));

    PAssert.that(grouped).satisfies(actual -> containsKIterableVs(expected, actual, new POJO[0]));
    pipeline.run();
  }

  /** A class for testing nested key grouping. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class OuterPOJO implements Serializable {
    public POJO inner;

    public OuterPOJO(POJO inner) {
      this.inner = inner;
    }

    public OuterPOJO() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OuterPOJO outerPOJO = (OuterPOJO) o;
      return Objects.equals(inner, outerPOJO.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }

    @Override
    public String toString() {
      return "OuterPOJO{" + "inner=" + inner + '}';
    }
  }

  /** Test grouping by a set of fields that are nested. */
  @Test
  @Category(NeedsRunner.class)
  public void testGroupByNestedKey() {
    PCollection<KV<Row, Iterable<OuterPOJO>>> grouped =
        pipeline
            .apply(
                Create.of(
                    new OuterPOJO(new POJO("key1", 1L, "value1")),
                    new OuterPOJO(new POJO("key1", 1L, "value2")),
                    new OuterPOJO(new POJO("key2", 2L, "value3")),
                    new OuterPOJO(new POJO("key2", 2L, "value4"))))
            .apply(Group.byFieldNames("inner.field1", "inner.field2"));

    Schema keySchema = Schema.builder().addStringField("field1").addInt64Field("field2").build();
    List<KV<Row, Collection<OuterPOJO>>> expected =
        ImmutableList.of(
            KV.of(
                Row.withSchema(keySchema).addValues("key1", 1L).build(),
                ImmutableList.of(
                    new OuterPOJO(new POJO("key1", 1L, "value1")),
                    new OuterPOJO(new POJO("key1", 1L, "value2")))),
            KV.of(
                Row.withSchema(keySchema).addValues("key2", 2L).build(),
                ImmutableList.of(
                    new OuterPOJO(new POJO("key2", 2L, "value3")),
                    new OuterPOJO(new POJO("key2", 2L, "value4")))));

    PAssert.that(grouped)
        .satisfies(actual -> containsKIterableVs(expected, actual, new OuterPOJO[0]));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupGlobally() {
    Collection<POJO> elements =
        ImmutableList.of(
            new POJO("key1", 1, "value1"),
            new POJO("key1", 1, "value2"),
            new POJO("key2", 2, "value3"),
            new POJO("key2", 2, "value4"));

    PCollection<Iterable<POJO>> grouped =
        pipeline.apply(Create.of(elements)).apply(Group.globally());
    PAssert.that(grouped).satisfies(actual -> containsSingleIterable(elements, actual));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGlobalAggregation() {
    Collection<POJO> elements =
        ImmutableList.of(
            new POJO("key1", 1, "value1"),
            new POJO("key1", 1, "value2"),
            new POJO("key2", 2, "value3"),
            new POJO("key2", 2, "value4"));
    PCollection<Long> count =
        pipeline
            .apply(Create.of(elements))
            .apply(Group.<POJO>globally().aggregate(Count.combineFn()));
    PAssert.that(count).containsInAnyOrder(4L);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPerKeyAggregation() {
    Collection<POJO> elements =
        ImmutableList.of(
            new POJO("key1", 1, "value1"),
            new POJO("key1", 1, "value2"),
            new POJO("key2", 2, "value3"),
            new POJO("key2", 2, "value4"),
            new POJO("key2", 2, "value4"));
    PCollection<KV<Row, Long>> count =
        pipeline
            .apply(Create.of(elements))
            .apply(Group.<POJO>byFieldNames("field1").aggregate(Count.combineFn()));

    Schema keySchema = Schema.builder().addStringField("field1").build();

    Collection<KV<Row, Long>> expectedCounts =
        ImmutableList.of(
            KV.of(Row.withSchema(keySchema).addValue("key1").build(), 2L),
            KV.of(Row.withSchema(keySchema).addValue("key2").build(), 3L));
    PAssert.that(count).containsInAnyOrder(expectedCounts);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputCoders() {
    Schema keySchema = Schema.builder().addStringField("field1").build();

    PCollection<KV<Row, Iterable<POJO>>> grouped =
        pipeline
            .apply(Create.of(new POJO("key1", 1, "value1")))
            .apply(Group.byFieldNames("field1"));

    // Make sure that the key has the right schema.
    PCollection<Row> keys = grouped.apply(Keys.create());
    assertTrue(keys.getSchema().equivalent(keySchema));

    // Make sure that the value has the right schema.
    PCollection<POJO> values = grouped.apply(Values.create()).apply(Flatten.iterables());
    assertTrue(values.getSchema().equivalent(POJO_SCHEMA));
    pipeline.run();
  }

  /** A class for testing field aggregation. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class AggregatePojos implements Serializable {
    public long field1;
    public long field2;
    public int field3;

    public AggregatePojos(long field1, long field2, int field3) {
      this.field1 = field1;
      this.field2 = field2;
      this.field3 = field3;
    }

    public AggregatePojos() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AggregatePojos agg = (AggregatePojos) o;
      return field1 == agg.field1 && field2 == agg.field2 && field3 == agg.field3;
    }

    @Override
    public int hashCode() {
      return Objects.hash(field1, field2, field3);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testByKeyWithSchemaAggregateFn() {
    Collection<AggregatePojos> elements =
        ImmutableList.of(
            new AggregatePojos(1, 1, 2),
            new AggregatePojos(2, 1, 3),
            new AggregatePojos(3, 2, 4),
            new AggregatePojos(4, 2, 5));

    PCollection<KV<Row, Row>> aggregations =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<AggregatePojos>byFieldNames("field2")
                    .aggregateField("field1", Sum.ofLongs(), "field1_sum")
                    .aggregateField("field3", Sum.ofIntegers(), "field3_sum")
                    .aggregateField("field1", Top.largestLongsFn(1), "field1_top"));

    Schema keySchema = Schema.builder().addInt64Field("field2").build();
    Schema valueSchema =
        Schema.builder()
            .addInt64Field("field1_sum")
            .addInt32Field("field3_sum")
            .addArrayField("field1_top", FieldType.INT64)
            .build();

    List<KV<Row, Row>> expected =
        ImmutableList.of(
            KV.of(
                Row.withSchema(keySchema).addValue(1L).build(),
                Row.withSchema(valueSchema).addValue(3L).addValue(5).addArray(2L).build()),
            KV.of(
                Row.withSchema(keySchema).addValue(2L).build(),
                Row.withSchema(valueSchema).addValue(7L).addValue(9).addArray(4L).build()));
    PAssert.that(aggregations).satisfies(actual -> containsKvs(expected, actual));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyWithSchemaAggregateFn() {
    Collection<AggregatePojos> elements =
        ImmutableList.of(
            new AggregatePojos(1, 1, 2),
            new AggregatePojos(2, 1, 3),
            new AggregatePojos(3, 2, 4),
            new AggregatePojos(4, 2, 5));

    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<AggregatePojos>globally()
                    .aggregateField("field1", Sum.ofLongs(), "field1_sum")
                    .aggregateField("field3", Sum.ofIntegers(), "field3_sum")
                    .aggregateField("field1", Top.largestLongsFn(1), "field1_top"));

    Schema aggregateSchema =
        Schema.builder()
            .addInt64Field("field1_sum")
            .addInt32Field("field3_sum")
            .addArrayField("field1_top", FieldType.INT64)
            .build();
    Row expectedRow = Row.withSchema(aggregateSchema).addValues(10L, 14).addArray(4L).build();
    PAssert.that(aggregate).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  /** A combine function that adds all long fields in the Row. */
  public static class MultipleFieldCombineFn extends CombineFn<Row, long[], Long> {
    @Override
    public long[] createAccumulator() {
      return new long[] {0};
    }

    @Override
    public long[] addInput(long[] accumulator, Row input) {
      for (Object o : input.getValues()) {
        if (o instanceof Long) {
          accumulator[0] += (Long) o;
        }
      }
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      Iterator<long[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        long[] running = iter.next();
        while (iter.hasNext()) {
          running[0] += iter.next()[0];
        }
        return running;
      }
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return accumulator[0];
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAggregateByMultipleFields() {
    Collection<AggregatePojos> elements =
        ImmutableList.of(
            new AggregatePojos(1, 1, 2),
            new AggregatePojos(2, 1, 3),
            new AggregatePojos(3, 2, 4),
            new AggregatePojos(4, 2, 5));

    List<String> fieldNames = Lists.newArrayList("field1", "field2");
    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<AggregatePojos>globally()
                    .aggregateFields(fieldNames, new MultipleFieldCombineFn(), "field1+field2"));

    Schema outputSchema = Schema.builder().addInt64Field("field1+field2").build();
    Row expectedRow = Row.withSchema(outputSchema).addValues(16L).build();
    PAssert.that(aggregate).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  /** A class for testing nested aggregation. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class OuterAggregate implements Serializable {
    public AggregatePojos inner;

    public OuterAggregate(AggregatePojos inner) {
      this.inner = inner;
    }

    public OuterAggregate() {}

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OuterAggregate that = (OuterAggregate) o;
      return Objects.equals(inner, that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testByKeyWithSchemaAggregateFnNestedFields() {
    Collection<OuterAggregate> elements =
        ImmutableList.of(
            new OuterAggregate(new AggregatePojos(1, 1, 2)),
            new OuterAggregate(new AggregatePojos(2, 1, 3)),
            new OuterAggregate(new AggregatePojos(3, 2, 4)),
            new OuterAggregate(new AggregatePojos(4, 2, 5)));

    PCollection<KV<Row, Row>> aggregations =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<OuterAggregate>byFieldNames("inner.field2")
                    .aggregateField("inner.field1", Sum.ofLongs(), "field1_sum")
                    .aggregateField("inner.field3", Sum.ofIntegers(), "field3_sum")
                    .aggregateField("inner.field1", Top.largestLongsFn(1), "field1_top"));

    Schema keySchema = Schema.builder().addInt64Field("field2").build();
    Schema valueSchema =
        Schema.builder()
            .addInt64Field("field1_sum")
            .addInt32Field("field3_sum")
            .addArrayField("field1_top", FieldType.INT64)
            .build();

    List<KV<Row, Row>> expected =
        ImmutableList.of(
            KV.of(
                Row.withSchema(keySchema).addValue(1L).build(),
                Row.withSchema(valueSchema).addValue(3L).addValue(5).addArray(2L).build()),
            KV.of(
                Row.withSchema(keySchema).addValue(2L).build(),
                Row.withSchema(valueSchema).addValue(7L).addValue(9).addArray(4L).build()));
    PAssert.that(aggregations).satisfies(actual -> containsKvs(expected, actual));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyWithSchemaAggregateFnNestedFields() {
    Collection<OuterAggregate> elements =
        ImmutableList.of(
            new OuterAggregate(new AggregatePojos(1, 1, 2)),
            new OuterAggregate(new AggregatePojos(2, 1, 3)),
            new OuterAggregate(new AggregatePojos(3, 2, 4)),
            new OuterAggregate(new AggregatePojos(4, 2, 5)));

    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<OuterAggregate>globally()
                    .aggregateField("inner.field1", Sum.ofLongs(), "field1_sum")
                    .aggregateField("inner.field3", Sum.ofIntegers(), "field3_sum")
                    .aggregateField("inner.field1", Top.largestLongsFn(1), "field1_top"));
    Schema aggregateSchema =
        Schema.builder()
            .addInt64Field("field1_sum")
            .addInt32Field("field3_sum")
            .addArrayField("field1_top", FieldType.INT64)
            .build();
    Row expectedRow = Row.withSchema(aggregateSchema).addValues(10L, 14).addArray(4L).build();
    PAssert.that(aggregate).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  private static <T> Void containsKIterableVs(
      List<KV<Row, Collection<T>>> expectedKvs,
      Iterable<KV<Row, Iterable<T>>> actualKvs,
      T[] emptyArray) {
    List<KV<Row, Iterable<T>>> list = Lists.newArrayList(actualKvs);
    List<Matcher<? super KV<Row, Iterable<POJO>>>> matchers = new ArrayList<>();
    for (KV<Row, Collection<T>> expected : expectedKvs) {
      T[] values = expected.getValue().toArray(emptyArray);
      matchers.add(isKv(equalTo(expected.getKey()), containsInAnyOrder(values)));
    }
    assertThat(actualKvs, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }

  private static <T> Void containsKvs(
      List<KV<Row, Row>> expectedKvs, Iterable<KV<Row, Row>> actualKvs) {
    List<Matcher<? super KV<Row, Iterable<POJO>>>> matchers = new ArrayList<>();
    for (KV<Row, Row> expected : expectedKvs) {
      matchers.add(isKv(equalTo(expected.getKey()), equalTo(expected.getValue())));
    }
    assertThat(actualKvs, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }

  private static Void containsSingleIterable(
      Collection<POJO> expected, Iterable<Iterable<POJO>> actual) {
    POJO[] values = expected.toArray(new POJO[0]);
    assertThat(actual, containsInAnyOrder(containsInAnyOrder(values)));
    return null;
  }
}
