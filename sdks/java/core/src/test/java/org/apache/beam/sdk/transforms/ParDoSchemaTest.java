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
import org.apache.beam.sdk.values.Row;
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
  public void testSchemaPipeline() {
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
}
