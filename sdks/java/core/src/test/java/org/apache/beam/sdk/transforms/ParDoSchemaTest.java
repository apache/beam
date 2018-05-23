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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test {@link Schema} support.
 */
@RunWith(JUnit4.class)
public class ParDoSchemaTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  static class MyPojo implements Serializable {
    MyPojo(String stringField, Integer integerField) {
      this.stringField = stringField;
      this.integerField = integerField;
    }

    String stringField;
    Integer integerField;
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleSchemaPipeline() {
    List<MyPojo> pojoList = Lists.newArrayList(
        new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema = Schema.builder()
        .addStringField("string_field", false)
        .addInt32Field("integer_field", false)
        .build();

    PCollection<String> output = pipeline
        .apply(Create.of(pojoList)
        .withSchema(schema,
            o -> Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
        .apply(ParDo.of(new DoFn<MyPojo, String>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<String> r) {
            r.output(row.getString(0) + ":" + row.getInt32(1));
          }
        }));
    PAssert.that(output)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadAndWrite() {
    List<MyPojo> pojoList = Lists.newArrayList(
        new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema1 = Schema.builder()
        .addStringField("string_field", false)
        .addInt32Field("integer_field", false)
        .build();

    Schema schema2 = Schema.builder()
        .addStringField("string2_field", false)
        .addInt32Field("integer2_field", false)
        .build();

    PCollection<String> output = pipeline
        .apply(Create.of(pojoList)
            .withSchema(schema1,
                o -> Row.withSchema(schema1).addValues(o.stringField, o.integerField).build(),
                r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
        .apply("first", ParDo.of(new DoFn<MyPojo, MyPojo>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<Row> r) {
            r.output(Row.withSchema(schema2).addValues(row.getString(0), row.getInt32(1))
                .build());
          }
        })).setSchema(schema2,
            o -> Row.withSchema(schema2).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string2_field"), r.getInt32("integer2_field")))
        .apply("second", ParDo.of(new DoFn<MyPojo, String>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<String> r) {
            r.output(row.getString(0) + ":" + row.getInt32(1));
          }
        }));
    PAssert.that(output)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadAndWriteMultiOutput() {
    List<MyPojo> pojoList = Lists.newArrayList(
        new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    Schema schema1 = Schema.builder()
        .addStringField("string_field", false)
        .addInt32Field("integer_field", false)
        .build();

    Schema schema2 = Schema.builder()
        .addStringField("string2_field", false)
        .addInt32Field("integer2_field", false)
        .build();

    Schema schema3 = Schema.builder()
        .addStringField("string3_field", false)
        .addInt32Field("integer3_field", false)
        .build();

    TupleTag<MyPojo> firstOutput = new TupleTag<>("first");
    TupleTag<MyPojo> secondOutput = new TupleTag<>("second");

    PCollectionTuple tuple = pipeline
        .apply(Create.of(pojoList)
            .withSchema(schema1,
                o -> Row.withSchema(schema1).addValues(o.stringField, o.integerField).build(),
                r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field"))))
        .apply("first", ParDo.of(new DoFn<MyPojo, MyPojo>() {
          @ProcessElement
          public void process(@Element Row row, MultiOutputReceiver r) {
            r.getRowReceiver(firstOutput).output(
                Row.withSchema(schema2).addValues(row.getString(0), row.getInt32(1))
                .build());
            r.getRowReceiver(secondOutput).output(
                Row.withSchema(schema3).addValues(row.getString(0), row.getInt32(1))
                    .build());
          }
        }).withOutputTags(firstOutput, TupleTagList.of(secondOutput)));
    tuple.get(firstOutput)
        .setSchema(schema2,
            o -> Row.withSchema(schema2).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string2_field"), r.getInt32("integer2_field")));
    tuple.get(secondOutput)
        .setSchema(schema3,
            o -> Row.withSchema(schema3).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string3_field"), r.getInt32("integer3_field")));

    PCollection<String> output1 = tuple.get(firstOutput)
        .apply("second", ParDo.of(new DoFn<MyPojo, String>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<String> r) {
            r.output(row.getString("string2_field") + ":" + row.getInt32("integer2_field"));
          }
        }));

    PCollection<String> output2 = tuple.get(secondOutput)
        .apply("third", ParDo.of(new DoFn<MyPojo, String>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<String> r) {
            r.output(row.getString("string3_field") + ":" + row.getInt32("integer3_field"));
          }
        }));

    PAssert.that(output1)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    PAssert.that(output2)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadAndWriteWithSchemaRegistry() {
    Schema schema = Schema.builder()
        .addStringField("string_field", false)
        .addInt32Field("integer_field", false)
        .build();

    pipeline.getSchemaRegistry()
        .registerSchemaForClass(MyPojo.class,
            schema,
            o -> Row.withSchema(schema).addValues(o.stringField, o.integerField).build(),
            r -> new MyPojo(r.getString("string_field"), r.getInt32("integer_field")));

    List<MyPojo> pojoList = Lists.newArrayList(
        new MyPojo("a", 1), new MyPojo("b", 2), new MyPojo("c", 3));

    PCollection<String> output = pipeline
        .apply(Create.of(pojoList))
        .apply("first", ParDo.of(new DoFn<MyPojo, MyPojo>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<Row> r) {
            r.output(Row.withSchema(schema).addValues(row.getString(0), row.getInt32(1))
                .build());
          }
        }))
        .apply("second", ParDo.of(new DoFn<MyPojo, String>() {
          @ProcessElement
          public void process(@Element Row row, OutputReceiver<String> r) {
            r.output(row.getString(0) + ":" + row.getInt32(1));
          }
        }));
    PAssert.that(output)
        .containsInAnyOrder("a:1", "b:2", "c:3");
    pipeline.run();
  }
}
