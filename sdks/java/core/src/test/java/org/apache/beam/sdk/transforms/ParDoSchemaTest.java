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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateKeySpec;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesMapState;
import org.apache.beam.sdk.testing.UsesOnWindowExpiration;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesTimerMap;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test {@link Schema} support. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class ParDoSchemaTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static class MyPojo implements Serializable {
    MyPojo(String stringField, Integer integerField) {
      this.stringField = stringField;
      this.integerField = integerField;
    }

    String stringField;
    Integer integerField;
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSimpleSchemaPipeline() {
    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    PCollection<String> output =
        pipeline
            .apply(
                Create.of(pojoList)
                    .withSchema(
                        schema,
                        TypeDescriptor.of(MyPojo.class),
                        o ->
                            Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
                        r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
            .apply(
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(row.getString(0) + ":" + row.getInt32(1));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadAndWrite() {
    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema1 =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    Schema schema2 =
        Schema.builder().addStringField("string2_field").addInt32Field("integer2_field").build();

    PCollection<String> output =
        pipeline
            .apply(
                Create.of(pojoList)
                    .withSchema(
                        schema1,
                        TypeDescriptor.of(MyPojo.class),
                        o ->
                            Row.withSchema(schema1)
                                .addValues(o.stringField, o.integerField)
                                .build(),
                        r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
            .apply(
                "first",
                ParDo.of(
                    new DoFn<MyPojo, MyPojo>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<Row> r) {
                        r.output(
                            Row.withSchema(schema2)
                                .addValues(row.getString(0), row.getInt32(1))
                                .build());
                      }
                    }))
            .setSchema(
                schema2,
                TypeDescriptor.of(MyPojo.class),
                o -> Row.withSchema(schema2).addValues(o.stringField, o.integerField).build(),
                r -> new MyPojo(r.getString("string2_field"), r.getInt32("integer2_field")))
            .apply(
                "second",
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(row.getString(0) + ":" + row.getInt32(1));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadAndWriteMultiOutput() {
    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema1 =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    Schema schema2 =
        Schema.builder().addStringField("string2_field").addInt32Field("integer2_field").build();

    Schema schema3 =
        Schema.builder().addStringField("string3_field").addInt32Field("integer3_field").build();

    TupleTag<MyPojo> firstOutput = new TupleTag<>("first");
    TupleTag<MyPojo> secondOutput = new TupleTag<>("second");

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(pojoList)
                    .withSchema(
                        schema1,
                        TypeDescriptor.of(MyPojo.class),
                        o ->
                            Row.withSchema(schema1)
                                .addValues(o.stringField, o.integerField)
                                .build(),
                        r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
            .apply(
                "first",
                ParDo.of(
                        new DoFn<MyPojo, MyPojo>() {
                          @ProcessElement
                          public void process(@Element Row row, MultiOutputReceiver r) {
                            r.getRowReceiver(firstOutput)
                                .output(
                                    Row.withSchema(schema2)
                                        .addValues(row.getString(0), row.getInt32(1))
                                        .build());
                            r.getRowReceiver(secondOutput)
                                .output(
                                    Row.withSchema(schema3)
                                        .addValues(row.getString(0), row.getInt32(1))
                                        .build());
                          }
                        })
                    .withOutputTags(firstOutput, TupleTagList.of(secondOutput)));
    tuple
        .get(firstOutput)
        .setSchema(
            schema2,
            TypeDescriptor.of(MyPojo.class),
            o -> Row.withSchema(schema2).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string2_field"), r.getInt32("integer2_field")));
    tuple
        .get(secondOutput)
        .setSchema(
            schema3,
            TypeDescriptor.of(MyPojo.class),
            o -> Row.withSchema(schema3).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string3_field"), r.getInt32("integer3_field")));

    PCollection<String> output1 =
        tuple
            .get(firstOutput)
            .apply(
                "second",
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(
                            row.getString("string2_field") + ":" + row.getInt32("integer2_field"));
                      }
                    }));

    PCollection<String> output2 =
        tuple
            .get(secondOutput)
            .apply(
                "third",
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(
                            row.getString("string3_field") + ":" + row.getInt32("integer3_field"));
                      }
                    }));

    PAssert.that(output1).containsInAnyOrder("a:1", "b:2", "c:3");
    PAssert.that(output2).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadAndWriteWithSchemaRegistry() {
    Schema schema =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    pipeline
        .getSchemaRegistry()
        .registerSchemaForClass(
            MyPojo.class,
            schema,
            o -> Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field")));

    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(
                "first",
                ParDo.of(
                    new DoFn<MyPojo, MyPojo>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<Row> r) {
                        r.output(
                            Row.withSchema(schema)
                                .addValues(row.getString(0), row.getInt32(1))
                                .build());
                      }
                    }))
            .apply(
                "second",
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(row.getString(0) + ":" + row.getInt32(1));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFieldAccessSchemaPipeline() {
    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    PCollection<String> output =
        pipeline
            .apply(
                Create.of(pojoList)
                    .withSchema(
                        schema,
                        TypeDescriptor.of(MyPojo.class),
                        o ->
                            Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
                        r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
            .apply(
                ParDo.of(
                    new DoFn<MyPojo, String>() {
                      @FieldAccess("foo")
                      final FieldAccessDescriptor fieldAccess =
                          FieldAccessDescriptor.withAllFields();

                      @ProcessElement
                      public void process(@FieldAccess("foo") Row row, OutputReceiver<String> r) {
                        r.output(row.getString(0) + ":" + row.getInt32(1));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testNoSchema() {
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of("a", "b", "c"))
        .apply(
            ParDo.of(
                new DoFn<String, Void>() {
                  @ProcessElement
                  public void process(@Element Row row) {}
                }));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testUnmatchedSchema() {
    List<MyPojo> pojoList =
        Lists.newArrayList(new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema =
        Schema.builder().addStringField("string_field").addInt32Field("integer_field").build();

    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(
            Create.of(pojoList)
                .withSchema(
                    schema,
                    TypeDescriptor.of(MyPojo.class),
                    o -> Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
                    r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
        .apply(
            ParDo.of(
                new DoFn<MyPojo, Void>() {
                  @FieldAccess("a")
                  FieldAccessDescriptor fieldAccess = FieldAccessDescriptor.withFieldNames("baad");

                  @ProcessElement
                  public void process(@FieldAccess("a") Row row) {}
                }));
    pipeline.run();
  }

  /** POJO used for testing. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Inferred {
    abstract String getStringField();

    abstract Integer getIntegerField();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testInferredSchemaPipeline() {
    List<Inferred> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_Inferred("a", 1),
            new AutoValue_ParDoSchemaTest_Inferred("b", 2),
            new AutoValue_ParDoSchemaTest_Inferred("c", 3));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(
                ParDo.of(
                    new DoFn<Inferred, String>() {
                      @ProcessElement
                      public void process(@Element Row row, OutputReceiver<String> r) {
                        r.output(row.getString("stringField") + ":" + row.getInt32("integerField"));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSchemasPassedThrough() {
    List<Inferred> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_Inferred("a", 1),
            new AutoValue_ParDoSchemaTest_Inferred("b", 2),
            new AutoValue_ParDoSchemaTest_Inferred("c", 3));

    PCollection<Inferred> out = pipeline.apply(Create.of(pojoList)).apply(Filter.by(e -> true));
    assertTrue(out.hasSchema());

    pipeline.run();
  }

  /** Pojo used for testing. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Inferred2 {
    abstract Integer getIntegerField();

    abstract String getStringField();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSchemaConversionPipeline() {
    List<Inferred> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_Inferred("a", 1),
            new AutoValue_ParDoSchemaTest_Inferred("b", 2),
            new AutoValue_ParDoSchemaTest_Inferred("c", 3));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(
                ParDo.of(
                    new DoFn<Inferred, String>() {
                      @ProcessElement
                      public void process(@Element Inferred2 pojo, OutputReceiver<String> r) {
                        r.output(pojo.getStringField() + ":" + pojo.getIntegerField());
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Nested {
    abstract int getField1();

    abstract Inferred getInner();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testNestedSchema() {
    List<Nested> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_Nested(1, new AutoValue_ParDoSchemaTest_Inferred("a", 1)),
            new AutoValue_ParDoSchemaTest_Nested(2, new AutoValue_ParDoSchemaTest_Inferred("b", 2)),
            new AutoValue_ParDoSchemaTest_Nested(
                3, new AutoValue_ParDoSchemaTest_Inferred("c", 3)));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(WithKeys.of("foo"))
            .apply(Reshuffle.of())
            .apply(Values.create())
            .apply(
                ParDo.of(
                    new DoFn<Nested, String>() {
                      @ProcessElement
                      public void process(@Element Nested nested, OutputReceiver<String> r) {
                        r.output(
                            nested.getInner().getStringField()
                                + ":"
                                + nested.getInner().getIntegerField());
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class ForExtraction {
    abstract Integer getIntegerField();

    abstract String getStringField();

    abstract List<Integer> getInts();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSchemaFieldSelectionUnboxing() {
    List<ForExtraction> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_ForExtraction(1, "a", Lists.newArrayList(1, 2)),
            new AutoValue_ParDoSchemaTest_ForExtraction(2, "b", Lists.newArrayList(2, 3)),
            new AutoValue_ParDoSchemaTest_ForExtraction(3, "c", Lists.newArrayList(3, 4)));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(
                ParDo.of(
                    new DoFn<ForExtraction, String>() {
                      // Read the list twice as two equivalent types to ensure that Beam properly
                      // converts.
                      @ProcessElement
                      public void process(
                          @FieldAccess("stringField") String stringField,
                          @FieldAccess("integerField") Integer integerField,
                          @FieldAccess("ints") Integer[] intArray,
                          @FieldAccess("ints") List<Integer> intList,
                          OutputReceiver<String> r) {

                        r.output(
                            stringField
                                + ":"
                                + integerField
                                + ":"
                                + Arrays.toString(intArray)
                                + ":"
                                + intList.toString());
                      }
                    }));
    PAssert.that(output)
        .containsInAnyOrder("a:1:[1, 2]:[1, 2]", "b:2:[2, 3]:[2, 3]", "c:3:[3, 4]:[3, 4]");
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSchemaFieldDescriptorSelectionUnboxing() {
    List<ForExtraction> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_ForExtraction(1, "a", Lists.newArrayList(1, 2)),
            new AutoValue_ParDoSchemaTest_ForExtraction(2, "b", Lists.newArrayList(2, 3)),
            new AutoValue_ParDoSchemaTest_ForExtraction(3, "c", Lists.newArrayList(3, 4)));

    PCollection<String> output =
        pipeline
            .apply(Create.of(pojoList))
            .apply(
                ParDo.of(
                    new DoFn<ForExtraction, String>() {
                      @FieldAccess("stringSelector")
                      final FieldAccessDescriptor stringSelector =
                          FieldAccessDescriptor.withFieldNames("stringField");

                      @FieldAccess("intSelector")
                      final FieldAccessDescriptor intSelector =
                          FieldAccessDescriptor.withFieldNames("integerField");

                      @FieldAccess("intsSelector")
                      final FieldAccessDescriptor intsSelector =
                          FieldAccessDescriptor.withFieldNames("ints");

                      @ProcessElement
                      public void process(
                          @FieldAccess("stringSelector") String stringField,
                          @FieldAccess("intSelector") int integerField,
                          @FieldAccess("intsSelector") int[] intArray,
                          OutputReceiver<String> r) {
                        r.output(
                            stringField + ":" + integerField + ":" + Arrays.toString(intArray));
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1:[1, 2]", "b:2:[2, 3]", "c:3:[3, 4]");
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class NestedForExtraction {
    abstract ForExtraction getInner();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testSchemaFieldSelectionNested() {
    List<ForExtraction> pojoList =
        Lists.newArrayList(
            new AutoValue_ParDoSchemaTest_ForExtraction(1, "a", Lists.newArrayList(1, 2)),
            new AutoValue_ParDoSchemaTest_ForExtraction(2, "b", Lists.newArrayList(2, 3)),
            new AutoValue_ParDoSchemaTest_ForExtraction(3, "c", Lists.newArrayList(3, 4)));
    List<NestedForExtraction> outerList =
        pojoList.stream()
            .map(AutoValue_ParDoSchemaTest_NestedForExtraction::new)
            .collect(Collectors.toList());

    PCollection<String> output =
        pipeline
            .apply(Create.of(outerList))
            .apply(
                ParDo.of(
                    new DoFn<NestedForExtraction, String>() {

                      @ProcessElement
                      public void process(
                          @FieldAccess("inner.*") ForExtraction extracted,
                          @FieldAccess("inner") ForExtraction extracted1,
                          @FieldAccess("inner.stringField") String stringField,
                          @FieldAccess("inner.integerField") int integerField,
                          @FieldAccess("inner.ints") List<Integer> intArray,
                          OutputReceiver<String> r) {
                        assertEquals(extracted, extracted1);
                        assertEquals(stringField, extracted.getStringField());
                        assertEquals(integerField, (int) extracted.getIntegerField());
                        assertEquals(intArray, extracted.getInts());
                        r.output(
                            extracted.getStringField()
                                + ":"
                                + extracted.getIntegerField()
                                + ":"
                                + extracted.getInts().toString());
                      }
                    }));
    PAssert.that(output).containsInAnyOrder("a:1:[1, 2]", "b:2:[2, 3]", "c:3:[3, 4]");
    pipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class})
  public void testRowBagState() {
    final String stateId = "foo";

    Schema type =
        Schema.builder()
            .addField(Schema.Field.of("f_string", FieldType.STRING))
            .addField(Schema.Field.of("key", FieldType.STRING))
            .build();

    Schema outputType = Schema.of(Field.of("values", FieldType.array(FieldType.row(type))));

    DoFn<Row, Row> fn =
        new DoFn<Row, Row>() {
          @StateId(stateId)
          private final StateSpec<BagState<Row>> bufferState = StateSpecs.rowBag(type);

          @StateKeyFields private final StateKeySpec keySpec = StateKeySpec.fields("key");

          @ProcessElement
          public void processElement(
              @Element Row element, @StateId(stateId) BagState<Row> state, OutputReceiver<Row> o) {
            state.add(element);
            Iterable<Row> currentValue = state.read();
            if (Iterables.size(currentValue) >= 4) {
              List<Row> sorted = Lists.newArrayList(currentValue);
              Collections.sort(sorted, Comparator.comparing(r -> r.getString(0)));
              o.output(Row.withSchema(outputType).addArray(sorted).build());
            }
          }
        };

    TupleTag<Row> mainTag = new TupleTag<>();
    PCollection<Row> output =
        pipeline
            .apply(
                "Create values",
                Create.of(
                        Row.withSchema(type).addValues("a", "hello").build(),
                        Row.withSchema(type).addValues("b", "hello").build(),
                        Row.withSchema(type).addValues("c", "hello").build(),
                        Row.withSchema(type).addValues("d", "hello").build())
                    .withRowSchema(type))
            .apply("run statetful fn", ParDo.of(fn).withOutputTags(mainTag, TupleTagList.empty()))
            .get(mainTag)
            .setRowSchema(outputType);
    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(outputType)
                .addArray(
                    Lists.newArrayList(
                        Row.withSchema(type).addValues("a", "hello").build(),
                        Row.withSchema(type).addValues("b", "hello").build(),
                        Row.withSchema(type).addValues("c", "hello").build(),
                        Row.withSchema(type).addValues("d", "hello").build()))
                .build());

    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class testRowBagStateKeyValue {
    public abstract String getKey();
  }

  public void testSchemaKeyProcessTimer(boolean isBounded) {
    final String timerId = "timer";
    final String timerFamilyId = "timerFamily";

    Schema type =
        Schema.builder()
            .addField(Schema.Field.of("f_string", FieldType.STRING))
            .addField(Schema.Field.of("key", FieldType.STRING))
            .build();

    Schema outputType =
        Schema.builder()
            .addField(Field.of("source", FieldType.STRING))
            .addField(Field.of("key", FieldType.STRING))
            .build();

    DoFn<Row, Row> fn =
        new DoFn<Row, Row>() {
          @TimerId(timerId)
          private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @StateKeyFields private final StateKeySpec keySpec = StateKeySpec.fields("key");

          @ProcessElement
          public void processElement(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              @Element Row element,
              @TimerId(timerId) Timer timer,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            assertEquals(autoValueKey.getKey(), element.getValue("key"));
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "processElement")
                    .withFieldValue("key", stringKey)
                    .build());
            timer.offset(Duration.standardSeconds(1)).setRelative();
          }

          @OnTimer(timerId)
          public void onTimer(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "onTimer")
                    .withFieldValue("key", stringKey)
                    .build());
          }
        };

    TupleTag<Row> mainTag = new TupleTag<>();
    PCollection<Row> output =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(type).addValues("a", "key1").build(),
                        Row.withSchema(type).addValues("b", "key2").build(),
                        Row.withSchema(type).addValues("c", "key3").build())
                    .withRowSchema(type))
            .setIsBoundedInternal(isBounded ? IsBounded.BOUNDED : IsBounded.UNBOUNDED)
            .apply(ParDo.of(fn).withOutputTags(mainTag, TupleTagList.empty()))
            .get(mainTag)
            .setRowSchema(outputType);
    PAssert.that(output)
        .containsInAnyOrder(
            Lists.newArrayList(
                Row.withSchema(type).addValues("processElement", "key1").build(),
                Row.withSchema(type).addValues("processElement", "key2").build(),
                Row.withSchema(type).addValues("processElement", "key3").build(),
                Row.withSchema(type).addValues("onTimer", "key1").build(),
                Row.withSchema(type).addValues("onTimer", "key2").build(),
                Row.withSchema(type).addValues("onTimer", "key3").build()));

    pipeline.run();
  }

  public void testSchemaKeyProcessTimerFamily(boolean isBounded) {
    final String timerId = "timer";
    final String timerFamilyId = "timerFamily";

    Schema type =
        Schema.builder()
            .addField(Schema.Field.of("f_string", FieldType.STRING))
            .addField(Schema.Field.of("key", FieldType.STRING))
            .build();

    Schema outputType =
        Schema.builder()
            .addField(Field.of("source", FieldType.STRING))
            .addField(Field.of("key", FieldType.STRING))
            .build();

    DoFn<Row, Row> fn =
        new DoFn<Row, Row>() {
          @TimerFamily(timerFamilyId)
          private final TimerSpec timerMapSpec = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

          @StateKeyFields private final StateKeySpec keySpec = StateKeySpec.fields("key");

          @ProcessElement
          public void processElement(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              @Element Row element,
              @TimerFamily(timerFamilyId) TimerMap timerMap,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            assertEquals(autoValueKey.getKey(), element.getValue("key"));
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "processElement")
                    .withFieldValue("key", stringKey)
                    .build());
            timerMap.get(timerId).offset(Duration.standardSeconds(1)).setRelative();
          }

          @OnTimerFamily(timerFamilyId)
          public void onTimerFamily(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "onTimerFamily")
                    .withFieldValue("key", stringKey)
                    .build());
          }
        };

    TupleTag<Row> mainTag = new TupleTag<>();
    PCollection<Row> output =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(type).addValues("a", "key1").build(),
                        Row.withSchema(type).addValues("b", "key2").build(),
                        Row.withSchema(type).addValues("c", "key3").build())
                    .withRowSchema(type))
            .setIsBoundedInternal(isBounded ? IsBounded.BOUNDED : IsBounded.UNBOUNDED)
            .apply(ParDo.of(fn).withOutputTags(mainTag, TupleTagList.empty()))
            .get(mainTag)
            .setRowSchema(outputType);
    PAssert.that(output)
        .containsInAnyOrder(
            Lists.newArrayList(
                Row.withSchema(type).addValues("processElement", "key1").build(),
                Row.withSchema(type).addValues("processElement", "key2").build(),
                Row.withSchema(type).addValues("processElement", "key3").build(),
                Row.withSchema(type).addValues("onTimerFamily", "key1").build(),
                Row.withSchema(type).addValues("onTimerFamily", "key2").build(),
                Row.withSchema(type).addValues("onTimerFamily", "key3").build()));

    pipeline.run();
  }

  public void testSchemaKeyOnWindowExpiration(boolean isBounded) {
    final String timerId = "timer";
    final String timerFamilyId = "timerFamily";

    Schema type =
        Schema.builder()
            .addField(Schema.Field.of("f_string", FieldType.STRING))
            .addField(Schema.Field.of("key", FieldType.STRING))
            .build();

    Schema outputType =
        Schema.builder()
            .addField(Field.of("source", FieldType.STRING))
            .addField(Field.of("key", FieldType.STRING))
            .build();

    DoFn<Row, Row> fn =
        new DoFn<Row, Row>() {
        final @StateId("foo") StateSpec<ValueState<Integer>> stateSpec = StateSpecs.value();
          @StateKeyFields private final StateKeySpec keySpec = StateKeySpec.fields("key");

          @ProcessElement
          public void processElement(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              @Element Row element,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            assertEquals(autoValueKey.getKey(), element.getValue("key"));
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "processElement")
                    .withFieldValue("key", stringKey)
                    .build());
          }

          @OnWindowExpiration
          public void onWindowExpiration(
              @Key Row key,
              @Key String stringKey,
              @Key testRowBagStateKeyValue autoValueKey,
              OutputReceiver<Row> o) {
            assertEquals(key.getValue(0), stringKey);
            assertEquals(stringKey, autoValueKey.getKey());
            o.output(
                Row.withSchema(outputType)
                    .withFieldValue("source", "onWindowExpiration")
                    .withFieldValue("key", stringKey)
                    .build());
          }
        };

    TupleTag<Row> mainTag = new TupleTag<>();
    PCollection<Row> output =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(type).addValues("a", "key1").build(),
                        Row.withSchema(type).addValues("b", "key2").build(),
                        Row.withSchema(type).addValues("c", "key3").build())
                    .withRowSchema(type))
            .setIsBoundedInternal(isBounded ? IsBounded.BOUNDED : IsBounded.UNBOUNDED)
            .apply(ParDo.of(fn).withOutputTags(mainTag, TupleTagList.empty()))
            .get(mainTag)
            .setRowSchema(outputType);
    PAssert.that(output)
        .containsInAnyOrder(
            Lists.newArrayList(
                Row.withSchema(type).addValues("processElement", "key1").build(),
                Row.withSchema(type).addValues("processElement", "key2").build(),
                Row.withSchema(type).addValues("processElement", "key3").build(),
                Row.withSchema(type).addValues("onWindowExpiration", "key1").build(),
                Row.withSchema(type).addValues("onWindowExpiration", "key2").build(),
                Row.withSchema(type).addValues("onWindowExpiration", "key3").build()));

    pipeline.run();
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesTimersInParDo.class,
  })
  public void testSchemaKeyProcessTimerBounded() {
    testSchemaKeyProcessTimer(true);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesTimersInParDo.class,
  })
  public void testSchemaKeyProcessTimerUnbounded() {
    testSchemaKeyProcessTimer(false);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesTimersInParDo.class,
    UsesTimerMap.class
  })
  public void testSchemaKeyProcessTimerFamilyBounded() {
    testSchemaKeyProcessTimerFamily(true);
  }

  @Test
  @Category({
    ValidatesRunner.class,
    UsesStatefulParDo.class,
    UsesTimersInParDo.class,
    UsesTimerMap.class
  })
  public void testSchemaKeyProcessTimerFamilyUnbounded() {
    testSchemaKeyProcessTimerFamily(false);
  }

  @Test
  @Category({ValidatesRunner.class, UsesOnWindowExpiration.class})
  public void testSchemaKeyOnWindowExpirationBounded() {
    testSchemaKeyOnWindowExpiration(true);
    ;
  }

  @Test
  @Category({ValidatesRunner.class, UsesOnWindowExpiration.class})
  public void testSchemaKeyOnWindowExpirationUnbounded() {
    testSchemaKeyOnWindowExpiration(false);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestStateSchemaValue {
    abstract String getName();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestStateSchemaValues {
    abstract List<TestStateSchemaValue> getValues();
  }

  @Test
  @Category({NeedsRunner.class, UsesStatefulParDo.class})
  public void tesBagStateSchemaInference() throws NoSuchSchemaException {
    final String stateId = "foo";

    DoFn<KV<String, TestStateSchemaValue>, TestStateSchemaValues> fn =
        new DoFn<KV<String, TestStateSchemaValue>, TestStateSchemaValues>() {

          // This should infer the schema.
          @StateId(stateId)
          private final StateSpec<BagState<TestStateSchemaValue>> bufferState = StateSpecs.bag();

          @ProcessElement
          public void processElement(
              @Element KV<String, TestStateSchemaValue> element,
              @StateId(stateId) BagState<TestStateSchemaValue> state,
              OutputReceiver<TestStateSchemaValues> o) {
            state.add(element.getValue());
            Iterable<TestStateSchemaValue> currentValue = state.read();
            if (Iterables.size(currentValue) >= 4) {
              List<TestStateSchemaValue> sorted = Lists.newArrayList(currentValue);
              Collections.sort(sorted, Comparator.comparing(TestStateSchemaValue::getName));
              o.output(new AutoValue_ParDoSchemaTest_TestStateSchemaValues(sorted));
            }
          }
        };

    PCollection<TestStateSchemaValue> input =
        pipeline.apply(
            Create.of(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d")));

    PCollection<TestStateSchemaValues> output =
        input.apply(WithKeys.of("hello")).apply(ParDo.of(fn));

    PAssert.that(output)
        .containsInAnyOrder(
            new AutoValue_ParDoSchemaTest_TestStateSchemaValues(
                Lists.newArrayList(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d"))));

    pipeline.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesStatefulParDo.class})
  public void testSetStateSchemaInference() throws NoSuchSchemaException {
    final String stateId = "foo";

    DoFn<KV<String, TestStateSchemaValue>, TestStateSchemaValues> fn =
        new DoFn<KV<String, TestStateSchemaValue>, TestStateSchemaValues>() {

          // This should infer the schema.
          @StateId(stateId)
          private final StateSpec<SetState<TestStateSchemaValue>> bufferState = StateSpecs.set();

          @ProcessElement
          public void processElement(
              @Element KV<String, TestStateSchemaValue> element,
              @StateId(stateId) SetState<TestStateSchemaValue> state,
              OutputReceiver<TestStateSchemaValues> o) {
            state.add(element.getValue());
            Iterable<TestStateSchemaValue> currentValue = state.read();
            if (Iterables.size(currentValue) >= 4) {
              List<TestStateSchemaValue> sorted = Lists.newArrayList(currentValue);
              Collections.sort(sorted, Comparator.comparing(TestStateSchemaValue::getName));
              o.output(new AutoValue_ParDoSchemaTest_TestStateSchemaValues(sorted));
            }
          }
        };

    PCollection<TestStateSchemaValue> input =
        pipeline.apply(
            Create.of(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d")));

    PCollection<TestStateSchemaValues> output =
        input.apply(WithKeys.of("hello")).apply(ParDo.of(fn));

    PAssert.that(output)
        .containsInAnyOrder(
            new AutoValue_ParDoSchemaTest_TestStateSchemaValues(
                Lists.newArrayList(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d"))));

    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestStateSchemaValue2 {
    abstract Integer getInteger();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TestStateSchemaMapEntry {
    abstract TestStateSchemaValue getKey();

    abstract TestStateSchemaValue2 getValue();
  }

  @Test
  @Category({ValidatesRunner.class, UsesStatefulParDo.class, UsesMapState.class})
  public void testMapStateSchemaInference() throws NoSuchSchemaException {
    final String stateId = "foo";
    final String countStateId = "count";

    DoFn<KV<String, TestStateSchemaMapEntry>, TestStateSchemaMapEntry> fn =
        new DoFn<KV<String, TestStateSchemaMapEntry>, TestStateSchemaMapEntry>() {

          @StateId(stateId)
          private final StateSpec<MapState<TestStateSchemaValue, TestStateSchemaValue2>> mapState =
              StateSpecs.map();

          @StateId(countStateId)
          private final StateSpec<CombiningState<Integer, int[], Integer>> countState =
              StateSpecs.combiningFromInputInternal(VarIntCoder.of(), Sum.ofIntegers());

          @ProcessElement
          public void processElement(
              @Element KV<String, TestStateSchemaMapEntry> element,
              @StateId(stateId) MapState<TestStateSchemaValue, TestStateSchemaValue2> state,
              @StateId(countStateId) CombiningState<Integer, int[], Integer> count,
              OutputReceiver<TestStateSchemaMapEntry> o) {
            TestStateSchemaMapEntry value = element.getValue();
            state.put(value.getKey(), value.getValue());
            count.add(1);
            if (count.read() >= 4) {
              Iterable<Map.Entry<TestStateSchemaValue, TestStateSchemaValue2>> iterate =
                  state.entries().read();
              for (Map.Entry<TestStateSchemaValue, TestStateSchemaValue2> entry : iterate) {
                o.output(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                        entry.getKey(), entry.getValue()));
              }
            }
          }
        };

    PCollection<TestStateSchemaMapEntry> input =
        pipeline.apply(
            Create.of(
                new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(1)),
                new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(2)),
                new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(3)),
                new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d"),
                    new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(4))));

    PCollection<TestStateSchemaMapEntry> output =
        input.apply(WithKeys.of("hello")).apply(ParDo.of(fn));

    PAssert.that(output)
        .containsInAnyOrder(
            new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("a"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(1)),
            new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("b"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(2)),
            new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("c"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(3)),
            new AutoValue_ParDoSchemaTest_TestStateSchemaMapEntry(
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue("d"),
                new AutoValue_ParDoSchemaTest_TestStateSchemaValue2(4)));
    pipeline.run();
  }
}
