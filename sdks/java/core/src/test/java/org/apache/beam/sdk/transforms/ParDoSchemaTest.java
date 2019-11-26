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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
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
}
