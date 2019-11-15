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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;

import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.mongodb.MongoDbTable.DocumentToRow;
import org.apache.beam.sdk.extensions.sql.meta.provider.mongodb.MongoDbTable.RowToDocument;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MongoDbTableTest {

  private static final Schema SCHEMA =
      Schema.builder()
          .addNullableField("long", INT64)
          .addNullableField("int32", INT32)
          .addNullableField("int16", INT16)
          .addNullableField("byte", BYTE)
          .addNullableField("bool", BOOLEAN)
          .addNullableField("double", DOUBLE)
          .addNullableField("float", FLOAT)
          .addNullableField("string", CalciteUtils.CHAR)
          .addRowField("nested", Schema.builder().addNullableField("int32", INT32).build())
          .addNullableField("arr", FieldType.array(STRING))
          .build();
  private static final String JSON_ROW =
      "{ "
          + "\"long\" : 9223372036854775807, "
          + "\"int32\" : 2147483647, "
          + "\"int16\" : 32767, "
          + "\"byte\" : 127, "
          + "\"bool\" : true, "
          + "\"double\" : 1.0, "
          + "\"float\" : 1.0, "
          + "\"string\" : \"string\", "
          + "\"nested\" : {\"int32\" : 2147483645}, "
          + "\"arr\" : [\"str1\", \"str2\", \"str3\"]"
          + " }";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testDocumentToRowConverter() {
    PCollection<Row> output =
        pipeline
            .apply("Create document from JSON", Create.<Document>of(Document.parse(JSON_ROW)))
            .apply("Convert document to Row", DocumentToRow.withSchema(SCHEMA));

    // Make sure proper rows are constructed from JSON.
    PAssert.that(output)
        .containsInAnyOrder(
            row(
                SCHEMA,
                9223372036854775807L,
                2147483647,
                (short) 32767,
                (byte) 127,
                true,
                1.0,
                (float) 1.0,
                "string",
                row(Schema.builder().addNullableField("int32", INT32).build(), 2147483645),
                Arrays.asList("str1", "str2", "str3")));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRowToDocumentConverter() {
    PCollection<Document> output =
        pipeline
            .apply(
                "Create a row",
                Create.of(
                        row(
                            SCHEMA,
                            9223372036854775807L,
                            2147483647,
                            (short) 32767,
                            (byte) 127,
                            true,
                            1.0,
                            (float) 1.0,
                            "string",
                            row(
                                Schema.builder().addNullableField("int32", INT32).build(),
                                2147483645),
                            Arrays.asList("str1", "str2", "str3")))
                    .withRowSchema(SCHEMA))
            .apply("Convert row to document", RowToDocument.convert());

    PAssert.that(output).containsInAnyOrder(Document.parse(JSON_ROW));

    pipeline.run().waitUntilFinish();
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
