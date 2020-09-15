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

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils.RowFieldMatcherIterableFieldAnyOrder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
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

  /** A basic type for testing. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Basic implements Serializable {
    abstract String getField1();

    abstract long getField2();

    abstract String getField3();

    static Basic of(String field1, long field2, String field3) {
      return new AutoValue_GroupTest_Basic(field1, field2, field3);
    }
  }

  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addStringField("field1")
          .addInt64Field("field2")
          .addStringField("field3")
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByOneField() throws NoSuchSchemaException {
    PCollection<Row> grouped =
        pipeline
            .apply(
                Create.of(
                    Basic.of("key1", 1, "value1"),
                    Basic.of("key1", 2, "value2"),
                    Basic.of("key2", 3, "value3"),
                    Basic.of("key2", 4, "value4")))
            .apply(Group.byFieldNames("field1"));

    Schema keySchema = Schema.builder().addStringField("field1").build();
    Schema outputSchema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addIterableField("value", FieldType.row(BASIC_SCHEMA))
            .build();

    SerializableFunction<Basic, Row> toRow =
        pipeline.getSchemaRegistry().getToRowFunction(Basic.class);

    List<Row> expected =
        ImmutableList.of(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue("key1").build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Basic.of("key1", 1L, "value1")),
                        toRow.apply(Basic.of("key1", 2L, "value2"))))
                .build(),
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue("key2").build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Basic.of("key2", 3L, "value3")),
                        toRow.apply(Basic.of("key2", 4L, "value4"))))
                .build());

    PAssert.that(grouped).satisfies(actual -> containsKIterableVs(expected, actual, new Basic[0]));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupByMultiple() throws NoSuchSchemaException {
    PCollection<Row> grouped =
        pipeline
            .apply(
                Create.of(
                    Basic.of("key1", 1, "value1"),
                    Basic.of("key1", 1, "value2"),
                    Basic.of("key2", 2, "value3"),
                    Basic.of("key2", 2, "value4")))
            .apply(Group.byFieldNames("field1", "field2"));

    Schema keySchema = Schema.builder().addStringField("field1").addInt64Field("field2").build();
    Schema outputSchema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addIterableField("value", FieldType.row(BASIC_SCHEMA))
            .build();
    SerializableFunction<Basic, Row> toRow =
        pipeline.getSchemaRegistry().getToRowFunction(Basic.class);

    List<Row> expected =
        ImmutableList.of(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValues("key1", 1L).build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Basic.of("key1", 1L, "value1")),
                        toRow.apply(Basic.of("key1", 1L, "value2"))))
                .build(),
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValues("key2", 2L).build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Basic.of("key2", 2L, "value3")),
                        toRow.apply(Basic.of("key2", 2L, "value4"))))
                .build());

    PAssert.that(grouped).satisfies(actual -> containsKIterableVs(expected, actual, new Basic[0]));
    pipeline.run();
  }

  /** A class for testing nested key grouping. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Outer implements Serializable {
    abstract Basic getInner();

    static Outer of(Basic inner) {
      return new AutoValue_GroupTest_Outer(inner);
    }
  }

  private static final Schema OUTER_SCHEMA =
      Schema.builder().addRowField("inner", BASIC_SCHEMA).build();

  /** Test grouping by a set of fields that are nested. */
  @Test
  @Category(NeedsRunner.class)
  public void testGroupByNestedKey() throws NoSuchSchemaException {
    PCollection<Row> grouped =
        pipeline
            .apply(
                Create.of(
                    Outer.of(Basic.of("key1", 1L, "value1")),
                    Outer.of(Basic.of("key1", 1L, "value2")),
                    Outer.of(Basic.of("key2", 2L, "value3")),
                    Outer.of(Basic.of("key2", 2L, "value4"))))
            .apply(Group.byFieldNames("inner.field1", "inner.field2"));

    Schema keySchema = Schema.builder().addStringField("field1").addInt64Field("field2").build();
    Schema outputSchema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addIterableField("value", FieldType.row(OUTER_SCHEMA))
            .build();

    SerializableFunction<Outer, Row> toRow =
        pipeline.getSchemaRegistry().getToRowFunction(Outer.class);

    List<Row> expected =
        ImmutableList.of(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValues("key1", 1L).build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Outer.of(Basic.of("key1", 1L, "value1"))),
                        toRow.apply(Outer.of(Basic.of("key1", 1L, "value2")))))
                .build(),
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValues("key2", 2L).build())
                .addIterable(
                    ImmutableList.of(
                        toRow.apply(Outer.of(Basic.of("key2", 2L, "value3"))),
                        toRow.apply(Outer.of(Basic.of("key2", 2L, "value4")))))
                .build());

    PAssert.that(grouped).satisfies(actual -> containsKIterableVs(expected, actual, new Outer[0]));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGroupGlobally() {
    Collection<Basic> elements =
        ImmutableList.of(
            Basic.of("key1", 1, "value1"),
            Basic.of("key1", 1, "value2"),
            Basic.of("key2", 2, "value3"),
            Basic.of("key2", 2, "value4"));

    PCollection<Iterable<Basic>> grouped =
        pipeline.apply(Create.of(elements)).apply(Group.globally());
    PAssert.that(grouped).satisfies(actual -> containsSingleIterable(elements, actual));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGlobalAggregation() {
    Collection<Basic> elements =
        ImmutableList.of(
            Basic.of("key1", 1, "value1"),
            Basic.of("key1", 1, "value2"),
            Basic.of("key2", 2, "value3"),
            Basic.of("key2", 2, "value4"));
    PCollection<Long> count =
        pipeline
            .apply(Create.of(elements))
            .apply(Group.<Basic>globally().aggregate(Count.combineFn()));
    PAssert.that(count).containsInAnyOrder(4L);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutputCoders() {
    Schema keySchema = Schema.builder().addStringField("field1").build();
    Schema outputSchema =
        Schema.builder()
            .addRowField("key", keySchema)
            .addIterableField("value", FieldType.row(BASIC_SCHEMA))
            .build();

    PCollection<Row> grouped =
        pipeline
            .apply(Create.of(Basic.of("key1", 1, "value1")))
            .apply(Group.byFieldNames("field1"));

    assertTrue(grouped.getSchema().equivalent(outputSchema));

    pipeline.run();
  }

  /** A class for testing field aggregation. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Aggregate implements Serializable {
    abstract long getField1();

    abstract long getField2();

    abstract int getField3();

    static Aggregate of(long field1, long field2, int field3) {
      return new AutoValue_GroupTest_Aggregate(field1, field2, field3);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testByKeyWithSchemaAggregateFn() {
    Collection<Aggregate> elements =
        ImmutableList.of(
            Aggregate.of(1, 1, 2),
            Aggregate.of(2, 1, 3),
            Aggregate.of(3, 2, 4),
            Aggregate.of(4, 2, 5));

    PCollection<Row> aggregations =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<Aggregate>byFieldNames("field2")
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
    Schema outputSchema =
        Schema.builder().addRowField("key", keySchema).addRowField("value", valueSchema).build();

    List<Row> expected =
        ImmutableList.of(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue(1L).build())
                .addValue(Row.withSchema(valueSchema).addValue(3L).addValue(5).addArray(2L).build())
                .build(),
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue(2L).build())
                .addValue(Row.withSchema(valueSchema).addValue(7L).addValue(9).addArray(4L).build())
                .build());
    PAssert.that(aggregations).satisfies(actual -> containsKvRows(expected, actual));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyWithSchemaAggregateFn() {
    Collection<Aggregate> elements =
        ImmutableList.of(
            Aggregate.of(1, 1, 2),
            Aggregate.of(2, 1, 3),
            Aggregate.of(3, 2, 4),
            Aggregate.of(4, 2, 5));

    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<Aggregate>globally()
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
    Collection<Aggregate> elements =
        ImmutableList.of(
            Aggregate.of(1, 1, 2),
            Aggregate.of(2, 1, 3),
            Aggregate.of(3, 2, 4),
            Aggregate.of(4, 2, 5));

    List<String> fieldNames = Lists.newArrayList("field1", "field2");
    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<Aggregate>globally()
                    .aggregateFields(fieldNames, new MultipleFieldCombineFn(), "field1+field2"));

    Schema outputSchema = Schema.builder().addInt64Field("field1+field2").build();
    Row expectedRow = Row.withSchema(outputSchema).addValues(16L).build();
    PAssert.that(aggregate).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  /** A class for testing nested aggregation. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class OuterAggregate implements Serializable {
    abstract Aggregate getInner();

    static OuterAggregate of(Aggregate inner) {
      return new AutoValue_GroupTest_OuterAggregate(inner);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testByKeyWithSchemaAggregateFnNestedFields() {
    Collection<OuterAggregate> elements =
        ImmutableList.of(
            OuterAggregate.of(Aggregate.of(1, 1, 2)),
            OuterAggregate.of(Aggregate.of(2, 1, 3)),
            OuterAggregate.of(Aggregate.of(3, 2, 4)),
            OuterAggregate.of(Aggregate.of(4, 2, 5)));

    PCollection<Row> aggregations =
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
    Schema outputSchema =
        Schema.builder().addRowField("key", keySchema).addRowField("value", valueSchema).build();

    List<Row> expected =
        ImmutableList.of(
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue(1L).build())
                .addValue(Row.withSchema(valueSchema).addValue(3L).addValue(5).addArray(2L).build())
                .build(),
            Row.withSchema(outputSchema)
                .addValue(Row.withSchema(keySchema).addValue(2L).build())
                .addValue(Row.withSchema(valueSchema).addValue(7L).addValue(9).addArray(4L).build())
                .build());
    PAssert.that(aggregations).satisfies(actual -> containsKvRows(expected, actual));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyWithSchemaAggregateFnNestedFields() {
    Collection<OuterAggregate> elements =
        ImmutableList.of(
            OuterAggregate.of(Aggregate.of(1, 1, 2)),
            OuterAggregate.of(Aggregate.of(2, 1, 3)),
            OuterAggregate.of(Aggregate.of(3, 2, 4)),
            OuterAggregate.of(Aggregate.of(4, 2, 5)));

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

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class BasicEnum {
    enum Test {
      ZERO,
      ONE,
      TWO
    };

    abstract String getKey();

    abstract Test getEnumeration();

    static BasicEnum of(String key, Test value) {
      return new AutoValue_GroupTest_BasicEnum(key, value);
    }
  }

  static final EnumerationType BASIC_ENUM_ENUMERATION =
      EnumerationType.create("ZERO", "ONE", "TWO");
  static final Schema BASIC_ENUM_SCHEMA =
      Schema.builder()
          .addStringField("key")
          .addLogicalTypeField("enumeration", BASIC_ENUM_ENUMERATION)
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testAggregateBaseValuesGlobally() {
    Collection<BasicEnum> elements =
        Lists.newArrayList(
            BasicEnum.of("a", BasicEnum.Test.ONE), BasicEnum.of("a", BasicEnum.Test.TWO));

    PCollection<Row> aggregate =
        pipeline
            .apply(Create.of(elements))
            .apply(
                Group.<BasicEnum>globally()
                    .aggregateFieldBaseValue("enumeration", Sum.ofIntegers(), "enum_sum"));
    Schema aggregateSchema = Schema.builder().addInt32Field("enum_sum").build();
    Row expectedRow = Row.withSchema(aggregateSchema).addValues(3).build();
    PAssert.that(aggregate).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAggregateLogicalValuesGlobally() {
    Collection<BasicEnum> elements =
        Lists.newArrayList(
            BasicEnum.of("a", BasicEnum.Test.ONE), BasicEnum.of("a", BasicEnum.Test.TWO));

    CombineFn<EnumerationType.Value, ?, Iterable<EnumerationType.Value>> sampleAnyCombineFn =
        Sample.anyCombineFn(100);
    Field aggField =
        Field.of("sampleList", FieldType.array(FieldType.logicalType(BASIC_ENUM_ENUMERATION)));
    pipeline
        .apply(Create.of(elements))
        .apply(
            Group.<BasicEnum>globally().aggregateField("enumeration", sampleAnyCombineFn, aggField))
        .apply(
            ParDo.of(
                new DoFn<Row, List<Integer>>() {
                  @ProcessElement
                  // TODO: List<enum> doesn't get converted properly by ConvertHelpers, so the
                  // following line does
                  // not work. TO fix this we need to move logical-type conversion out of
                  // RowWithGetters and into
                  // the actual getters.
                  //    public void process(@FieldAccess("sampleList") List<BasicEnum.Test> values)
                  // {
                  public void process(@Element Row value) {
                    assertThat(
                        value.getArray(0),
                        containsInAnyOrder(
                            BASIC_ENUM_ENUMERATION.valueOf(1), BASIC_ENUM_ENUMERATION.valueOf(2)));
                  }
                }));

    pipeline.run();
  }

  private static <T> Void containsKIterableVs(
      List<Row> expectedKvs, Iterable<Row> actualKvs, T[] emptyArray) {
    List<Row> list = Lists.newArrayList(actualKvs);
    List<Matcher<? super Row>> matchers = new ArrayList<>();
    for (Row expected : expectedKvs) {
      List<Matcher> fieldMatchers = Lists.newArrayList();
      fieldMatchers.add(
          new RowFieldMatcherIterableFieldAnyOrder(expected.getSchema(), 0, expected.getRow(0)));
      assertEquals(TypeName.ITERABLE, expected.getSchema().getField(1).getType().getTypeName());
      fieldMatchers.add(
          new RowFieldMatcherIterableFieldAnyOrder(
              expected.getSchema(), 1, expected.getIterable(1)));
      matchers.add(allOf(fieldMatchers.toArray(new Matcher[0])));
    }
    assertThat(actualKvs, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }

  private static <T> Void containsKvRows(List<Row> expectedKvs, Iterable<Row> actualKvs) {
    List<Matcher<? super Row>> matchers = new ArrayList<>();
    for (Row expected : expectedKvs) {
      matchers.add(new KvRowMatcher(equalTo(expected.getRow(0)), equalTo(expected.getRow(1))));
    }
    assertThat(actualKvs, containsInAnyOrder(matchers.toArray(new Matcher[0])));
    return null;
  }

  public static class KvRowMatcher extends TypeSafeMatcher<Row> {
    final Matcher<? super Row> keyMatcher;
    final Matcher<? super Row> valueMatcher;

    public KvRowMatcher(Matcher<? super Row> keyMatcher, Matcher<? super Row> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matchesSafely(Row kvRow) {
      return keyMatcher.matches(kvRow.getRow(0)) && valueMatcher.matches(kvRow.getRow(1));
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a KVRow(")
          .appendValue(keyMatcher)
          .appendText(", ")
          .appendValue(valueMatcher)
          .appendText(")");
    }
  }

  private static Void containsSingleIterable(
      Collection<Basic> expected, Iterable<Iterable<Basic>> actual) {
    Basic[] values = expected.toArray(new Basic[0]);
    assertThat(actual, containsInAnyOrder(containsInAnyOrder(values)));
    return null;
  }
}
