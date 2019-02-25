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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

public class AddFieldsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void addSimpleFields() {
    Schema schema = Schema.builder().addStringField("field1").build();
    PCollection<Row> added =
        pipeline
            .apply(
                Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
            .apply(
                AddFields.fields(
                    Schema.Field.nullable("field2", Schema.FieldType.INT16),
                    Schema.Field.nullable(
                        "field3", Schema.FieldType.array(Schema.FieldType.STRING))));

    Schema expectedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT16)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .build();
    assertEquals(expectedSchema, added.getSchema());
    Row expected = Row.withSchema(expectedSchema).addValues("value", null, null).build();
    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addDuplicateField() {
    Schema schema = Schema.builder().addStringField("field1").build();
    thrown.expect(IllegalArgumentException.class);
    PCollection<Row> added =
        pipeline
            .apply(
                Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
            .apply(AddFields.fields(Schema.Field.nullable("field1", Schema.FieldType.INT16)));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNonNullableField() {
    Schema schema = Schema.builder().addStringField("field1").build();
    thrown.expect(IllegalArgumentException.class);
    PCollection<Row> added =
        pipeline
            .apply(
                Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
            .apply(AddFields.fields(Schema.Field.of("field2", Schema.FieldType.INT16)));
    pipeline.run();
  }
}
