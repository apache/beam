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

import java.io.Serializable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JsonToRow}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class JsonToRowTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testParsesRows() throws Exception {
    Schema personSchema =
        Schema.builder()
            .addStringField("name")
            .addInt32Field("height")
            .addBooleanField("knowsJavascript")
            .build();

    PCollection<String> jsonPersons =
        pipeline.apply(
            "jsonPersons",
            Create.of(
                jsonPerson("person1", "80", "true"),
                jsonPerson("person2", "70", "false"),
                jsonPerson("person3", "60", "true"),
                jsonPerson("person4", "50", "false"),
                jsonPerson("person5", "40", "true")));

    PCollection<Row> personRows =
        jsonPersons.apply(JsonToRow.withSchema(personSchema)).setRowSchema(personSchema);

    PAssert.that(personRows)
        .containsInAnyOrder(
            row(personSchema, "person1", 80, true),
            row(personSchema, "person2", 70, false),
            row(personSchema, "person3", 60, true),
            row(personSchema, "person4", 50, false),
            row(personSchema, "person5", 40, true));

    pipeline.run();
  }

  private String jsonPerson(String name, String height, String knowsJs) {
    return "{\n"
        + "  \"name\": \""
        + name
        + "\",\n"
        + "  \"height\": "
        + height
        + ",\n"
        + "  \"knowsJavascript\": "
        + knowsJs
        + "\n"
        + "}";
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
