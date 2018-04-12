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
package org.apache.beam.sdk.extensions.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/**
 * JsonToRowSqlTest.
 */
public class JsonToRowSqlTest {
  private static final boolean NOT_NULLABLE = false;

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParsesRows() throws Exception {
    Schema personSchema =
        Schema
            .builder()
            .addStringField("name", NOT_NULLABLE)
            .addInt32Field("height", NOT_NULLABLE)
            .addBooleanField("knowsJavascript", NOT_NULLABLE)
            .build();

    PCollection<String> jsonPersons =
        pipeline.apply(
            Create
                .of(
                    jsonPerson("person1", "80", "true"),
                    jsonPerson("person2", "70", "false"),
                    jsonPerson("person3", "60", "true"),
                    jsonPerson("person4", "50", "false"),
                    jsonPerson("person5", "40", "true")));

    Schema resultSchema =
        Schema
            .builder()
            .addInt32Field("avg_height", NOT_NULLABLE)
            .build();

    PCollection<Row> sqlResult =
        jsonPersons
            .apply(JsonToRow.withSchema(personSchema))
            .setCoder(personSchema.getRowCoder())
            .apply(BeamSql.query("SELECT AVG(height) as avg_height FROM PCOLLECTION"));

    PAssert
        .that(sqlResult)
        .containsInAnyOrder(
            row(resultSchema, 60));

    pipeline.run();
  }

  private String jsonPerson(String name, String height, String knowsJs) {
    return
        "{\n"
        + "  \"name\": \"" + name + "\",\n"
        + "  \"height\": " + height + ",\n"
        + "  \"knowsJavascript\": " + knowsJs + "\n"
        + "}";
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
