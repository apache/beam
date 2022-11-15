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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.apache.beam.sdk.io.gcp.firestore.FirestoreHelper.makeStringValue;
import static org.apache.beam.sdk.io.gcp.firestore.FirestoreHelper.makeValue;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTES;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DATETIME;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.schemas.Schema.FieldType.array;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DocumentToRowRowToDocumentTest {
  private static final String KEY = UUID.randomUUID().toString();
  private static final DateTime DATE_TIME =
      parseTimestampWithUTCTimeZone("2018-05-28 20:17:40.567");
  static final String DEFAULT_KEY_FIELD = "firestoreKey";

  final Logger log = Logger.getLogger(DocumentToRowRowToDocumentTest.class.getName());

  private static final Schema NESTED_ROW_SCHEMA =
      Schema.builder().addNullableField("nestedLong", INT64).build();
  private static final Schema SCHEMA =
      Schema.builder()
          .addNullableField(DEFAULT_KEY_FIELD, STRING)
          .addNullableField("long", INT64)
          .addNullableField("bool", BOOLEAN)
          .addNullableField("datetime", DATETIME)
          .addNullableField("array", array(STRING))
          .addNullableField("rowArray", array(FieldType.row(NESTED_ROW_SCHEMA)))
          .addNullableField("double", DOUBLE)
          .addNullableField("bytes", BYTES)
          .addNullableField("string", STRING)
          .addNullableField("nullable", INT64)
          .build();
  private static final MapValue NESTED_ENTITY =
      MapValue.newBuilder().putFields("nestedLong", makeValue(Long.MIN_VALUE).build()).build();
  private static final Document ENTITY =
      Document.newBuilder()
          .setName(KEY)
          .putFields("long", makeValue(Long.MAX_VALUE).build())
          .putFields("bool", makeValue(true).build())
          .putFields("datetime", makeValue(DATE_TIME.toDate()).build())
          .putFields(
              "array", makeValue(makeStringValue("string1"), makeStringValue("string2")).build())
          .putFields(
              "rowArray",
              makeValue(Collections.singletonList(makeValue(NESTED_ENTITY).build())).build())
          .putFields("double", makeValue(Double.MAX_VALUE).build())
          .putFields(
              "bytes", makeValue(ByteString.copyFrom("hello", StandardCharsets.UTF_8)).build())
          .putFields("string", makeStringValue("string").build())
          .putFields("nullable", Value.newBuilder().build())
          .build();
  private static final Row ROW =
      row(
          SCHEMA,
          KEY,
          Long.MAX_VALUE,
          true,
          DATE_TIME,
          Arrays.asList("string1", "string2"),
          Collections.singletonList(row(NESTED_ROW_SCHEMA, Long.MIN_VALUE)),
          Double.MAX_VALUE,
          "hello".getBytes(StandardCharsets.UTF_8),
          "string",
          null);

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testEntityToRowConverter() {
    PCollection<Row> result =
        pipeline.apply(Create.of(ENTITY)).apply(DocumentToRow.create(SCHEMA, DEFAULT_KEY_FIELD));
    PAssert.that(result).containsInAnyOrder(ROW);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRowToEntityConverter() {
    PCollection<Document> result =
        pipeline
            .apply(Create.of(ROW))
            .setRowSchema(SCHEMA)
            .apply(RowToDocument.create(DEFAULT_KEY_FIELD));
    PAssert.that(result).containsInAnyOrder(ENTITY);

    pipeline.run().waitUntilFinish();
  }

  @Test(expected = IllegalStateException.class)
  public void testConverterWithoutKey() {
    Schema schemaWithoutKey =
        Schema.builder()
            .addFields(
                SCHEMA.getFields().stream()
                    .filter(f -> !f.getName().equals(DEFAULT_KEY_FIELD))
                    .collect(Collectors.toList()))
            .build();
    RowToDocument.validateKeyFieldPresenceAndType(schemaWithoutKey, DEFAULT_KEY_FIELD);
  }

  private static Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  public static DateTime parseTimestampWithUTCTimeZone(String str) {
    if (str.indexOf('.') == -1) {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC().parseDateTime(str);
    } else {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC().parseDateTime(str);
    }
  }
}
