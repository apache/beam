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
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.JsonToRow.JsonToRowFn;
import org.apache.beam.sdk.transforms.JsonToRow.JsonToRowWithErrFn;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.NullBehavior;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JsonToRow}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class JsonToRowTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Schema PERSON_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addInt32Field("height")
          .addBooleanField("knowsJavascript")
          .build();

  private static final ImmutableList<String> JSON_PERSON =
      ImmutableList.of(
          jsonPerson("person1", "80", "true"),
          jsonPerson("person2", "70", "false"),
          jsonPerson("person3", "60", "true"),
          jsonPerson("person4", "50", "false"),
          jsonPerson("person5", "40", "true"));

  private static final ImmutableList<Row> PERSON_ROWS =
      ImmutableList.of(
          row(PERSON_SCHEMA, "person1", 80, true),
          row(PERSON_SCHEMA, "person2", 70, false),
          row(PERSON_SCHEMA, "person3", 60, true),
          row(PERSON_SCHEMA, "person4", 50, false),
          row(PERSON_SCHEMA, "person5", 40, true));

  private static final Schema PERSON_SCHEMA_WITH_NULLABLE_FIELD =
      Schema.builder()
          .addStringField("name")
          .addInt32Field("height")
          .addNullableField("knowsJavascript", FieldType.BOOLEAN)
          .build();

  private static final ImmutableList<String> JSON_PERSON_WITH_IMPLICIT_NULLS =
      ImmutableList.of(
          jsonPerson("person1", "80"),
          jsonPerson("person2", "70", "false"),
          jsonPerson("person3", "60"),
          jsonPerson("person4", "50", "false"),
          jsonPerson("person5", "40", "true"));

  private static final ImmutableList<Row> PERSON_ROWS_WITH_NULLS =
      ImmutableList.of(
          row(PERSON_SCHEMA_WITH_NULLABLE_FIELD, "person1", 80, null),
          row(PERSON_SCHEMA_WITH_NULLABLE_FIELD, "person2", 70, false),
          row(PERSON_SCHEMA_WITH_NULLABLE_FIELD, "person3", 60, null),
          row(PERSON_SCHEMA_WITH_NULLABLE_FIELD, "person4", 50, false),
          row(PERSON_SCHEMA_WITH_NULLABLE_FIELD, "person5", 40, true));

  private static final ImmutableList<String> JSON_PERSON_WITH_ERR =
      ImmutableList.copyOf(
          Stream.of(ImmutableList.of("{}", "Is it 42?"), JSON_PERSON)
              .flatMap(Collection::stream)
              .collect(Collectors.toList()));

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

  @Test
  @Category(NeedsRunner.class)
  public void testParsesRowsMissing() throws Exception {

    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_IMPLICIT_NULLS));

    PCollection<Row> personRows =
        jsonPersons
            .apply(
                ((JsonToRowFn)
                    JsonToRow.withSchemaAndNullBehavior(
                        PERSON_SCHEMA_WITH_NULLABLE_FIELD, NullBehavior.ACCEPT_MISSING_OR_NULL)))
            .setRowSchema(PERSON_SCHEMA_WITH_NULLABLE_FIELD);

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS_WITH_NULLS);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesRowsDeadLetterNoErrors() throws Exception {

    PCollection<String> jsonPersons = pipeline.apply("jsonPersons", Create.of(JSON_PERSON));

    ParseResult results = jsonPersons.apply(JsonToRow.withExceptionReporting(PERSON_SCHEMA));

    PCollection<Row> personRows = results.getResults();
    PCollection<Row> errors = results.getFailedToParseLines();

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS);

    PAssert.that(errors).empty();

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesRowsDeadLetterWithMissingFieldsNoErrors() throws Exception {

    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_IMPLICIT_NULLS));

    ParseResult results =
        jsonPersons.apply(
            JsonToRow.withExceptionReporting(PERSON_SCHEMA_WITH_NULLABLE_FIELD)
                .withNullBehavior(NullBehavior.ACCEPT_MISSING_OR_NULL));

    PCollection<Row> personRows = results.getResults();
    PCollection<Row> errors = results.getFailedToParseLines();

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS_WITH_NULLS);

    PAssert.that(errors).empty();

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesErrorRowsDeadLetter() throws Exception {

    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_ERR));

    ParseResult results = jsonPersons.apply(JsonToRow.withExceptionReporting(PERSON_SCHEMA));

    PCollection<Row> personRows = results.getResults();
    PCollection<Row> errors = results.getFailedToParseLines();

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS);

    PAssert.that(errors)
        .containsInAnyOrder(
            row(JsonToRowWithErrFn.ERROR_ROW_SCHEMA, "{}"),
            row(JsonToRow.JsonToRowWithErrFn.ERROR_ROW_SCHEMA, "Is it 42?"));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesErrorWithErrorMsgRowsDeadLetter() throws Exception {
    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_ERR));

    ParseResult results =
        jsonPersons.apply(JsonToRow.withExceptionReporting(PERSON_SCHEMA).withExtendedErrorInfo());

    PCollection<Row> personRows = results.getResults();
    PCollection<Row> errorsWithMsg = results.getFailedToParseLines();

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS);

    PAssert.that(errorsWithMsg)
        .containsInAnyOrder(
            row(
                JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA,
                "{}",
                "Non-nullable field 'name' is not present in the JSON object."),
            row(
                JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA,
                "Is it 42?",
                "Unable to parse Row"));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesErrorWithErrorMsgWithRequireNullDeadLetter() throws Exception {
    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_IMPLICIT_NULLS));

    ParseResult results =
        jsonPersons.apply(
            JsonToRow.withExceptionReporting(PERSON_SCHEMA_WITH_NULLABLE_FIELD)
                .withExtendedErrorInfo()
                .withNullBehavior(NullBehavior.REQUIRE_NULL));

    PCollection<Row> errorsWithMsg = results.getFailedToParseLines();

    PAssert.that(errorsWithMsg)
        .containsInAnyOrder(
            row(
                JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA,
                jsonPerson("person1", "80"),
                "Field 'knowsJavascript' is not present in the JSON object."),
            row(
                JsonToRowWithErrFn.ERROR_ROW_WITH_ERR_MSG_SCHEMA,
                jsonPerson("person3", "60"),
                "Field 'knowsJavascript' is not present in the JSON object."));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testParsesErrorWithErrorMsgRowsDeadLetterWithCustomFieldNames() throws Exception {
    PCollection<String> jsonPersons =
        pipeline.apply("jsonPersons", Create.of(JSON_PERSON_WITH_ERR));

    String customErrField = "CUSTOM_ERR";
    String customLineField = "CUSTOM_LINE";

    ParseResult results =
        jsonPersons.apply(
            JsonToRow.withExceptionReporting(PERSON_SCHEMA)
                .withExtendedErrorInfo()
                .setErrorField(customErrField)
                .setLineField(customLineField));

    Schema customSchema =
        Schema.builder()
            .addField(customLineField, FieldType.STRING)
            .addField(customErrField, FieldType.STRING)
            .build();

    PCollection<Row> personRows = results.getResults();
    PCollection<Row> errorsWithMsg = results.getFailedToParseLines();

    PAssert.that(personRows).containsInAnyOrder(PERSON_ROWS);

    PAssert.that(errorsWithMsg)
        .containsInAnyOrder(
            row(customSchema, "{}", "Non-nullable field 'name' is not present in the JSON object."),
            row(customSchema, "Is it 42?", "Unable to parse Row"));

    pipeline.run();
  }

  private static String jsonPerson(String name, String height, String knowsJs) {
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

  private static String jsonPerson(String name, String height) {
    return "{\n" + "  \"name\": \"" + name + "\",\n" + "  \"height\": " + height + "}";
  }

  private static Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
