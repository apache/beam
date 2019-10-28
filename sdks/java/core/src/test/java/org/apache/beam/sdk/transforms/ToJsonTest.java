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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
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
public class ToJsonTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Person {
    public static Person of(String name, Integer height, Boolean knowsJavascript) {
      return new AutoValue_ToJsonTest_Person(name, height, knowsJavascript);
    }

    public abstract String getName();

    public abstract Integer getHeight();

    public abstract Boolean getKnowsJavascript();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSerializesParseableJson() throws Exception {
    PCollection<Person> persons =
        pipeline.apply(
            "jsonPersons",
            Create.of(
                Person.of("person1", 80, true),
                Person.of("person2", 70, false),
                Person.of("person3", 60, true),
                Person.of("person4", 50, false),
                Person.of("person5", 40, true)));

    Schema personSchema =
        Schema.builder()
            .addStringField("name")
            .addInt32Field("height")
            .addBooleanField("knowsJavascript")
            .build();

    PCollection<Row> personRows =
        persons.apply(ToJson.of()).apply(JsonToRow.withSchema(personSchema));

    PAssert.that(personRows)
        .containsInAnyOrder(
            row(personSchema, "person1", 80, true),
            row(personSchema, "person2", 70, false),
            row(personSchema, "person3", 60, true),
            row(personSchema, "person4", 50, false),
            row(personSchema, "person5", 40, true));

    pipeline.run();
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
