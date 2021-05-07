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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static org.junit.Assert.assertEquals;

import com.google.cloud.datacatalog.v1beta1.ColumnSchema;
import com.google.cloud.datacatalog.v1beta1.PhysicalSchema;
import com.google.cloud.datacatalog.v1beta1.PhysicalSchema.AvroSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SchemaUtils}. */
@RunWith(JUnit4.class)
public class SchemaUtilsTest {

  private static final Schema TEST_INNER_SCHEMA =
      Schema.builder().addField("i1", FieldType.INT64).addField("i2", FieldType.STRING).build();

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addNullableField("f_int32", FieldType.INT32)
          .addNullableField("f_int64", FieldType.INT64)
          .addNullableField("f_bytes", FieldType.BYTES)
          .addNullableField("f_double", FieldType.DOUBLE)
          .addNullableField("f_string", FieldType.STRING)
          .addNullableField("f_bool", FieldType.BOOLEAN)
          .addNullableField("f_ts", FieldType.DATETIME)
          .addNullableField("f_numeric", FieldType.DECIMAL)
          .addLogicalTypeField("f_time", SqlTypes.TIME)
          .addLogicalTypeField("f_date", SqlTypes.DATE)
          .addLogicalTypeField("f_datetime", SqlTypes.DATETIME)
          .addArrayField("f_array", FieldType.INT64)
          .addRowField("f_struct", TEST_INNER_SCHEMA)
          .build();

  private static final com.google.cloud.datacatalog.v1beta1.Schema TEST_DC_SCHEMA =
      com.google.cloud.datacatalog.v1beta1.Schema.newBuilder()
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_int32")
                  .setType("INT32")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_int64")
                  .setType("INT64")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_bytes")
                  .setType("BYTES")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_double")
                  .setType("DOUBLE")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_string")
                  .setType("STRING")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_bool")
                  .setType("BOOL")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_ts")
                  .setType("TIMESTAMP")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_numeric")
                  .setType("NUMERIC")
                  .setMode("NULLABLE")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_time")
                  .setType("TIME")
                  .setMode("REQUIRED")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_date")
                  .setType("DATE")
                  .setMode("REQUIRED")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_datetime")
                  .setType("DATETIME")
                  .setMode("REQUIRED")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_array")
                  .setType("INT64")
                  .setMode("REPEATED")
                  .build())
          .addColumns(
              ColumnSchema.newBuilder()
                  .setColumn("f_struct")
                  .setType("STRUCT")
                  .addSubcolumns(
                      ColumnSchema.newBuilder()
                          .setColumn("i1")
                          .setType("INT64")
                          .setMode("REQUIRED")
                          .build())
                  .addSubcolumns(
                      ColumnSchema.newBuilder()
                          .setColumn("i2")
                          .setType("STRING")
                          .setMode("REQUIRED")
                          .build())
                  .setMode("REQUIRED")
                  .build())
          .build();

  private static final Schema TEST_SCHEMA2 =
      Schema.builder()
          .addRowField(
              "r",
              Schema.builder()
                  .addField("s", FieldType.STRING)
                  .addField("l", FieldType.INT64)
                  .addField("d", FieldType.DOUBLE)
                  .addField("b", FieldType.BOOLEAN)
                  .build())
          .build();

  private static final com.google.cloud.datacatalog.v1beta1.Schema TEST_DC_SCHEMA2 =
      com.google.cloud.datacatalog.v1beta1.Schema.newBuilder()
          .setPhysicalSchema(
              PhysicalSchema.newBuilder()
                  .setAvro(
                      AvroSchema.newBuilder()
                          .setText(
                              "{\n"
                                  + "  \"type\": \"record\",\n"
                                  + "  \"name\": \"r\",\n"
                                  + "  \"fields\": [\n"
                                  + "    {\n"
                                  + "      \"name\": \"s\",\n"
                                  + "      \"type\": \"string\"\n"
                                  + "    },\n"
                                  + "    {\n"
                                  + "      \"name\": \"l\",\n"
                                  + "      \"type\": \"long\"\n"
                                  + "    },\n"
                                  + "    {\n"
                                  + "      \"name\": \"d\",\n"
                                  + "      \"type\": \"double\"\n"
                                  + "    },\n"
                                  + "    {\n"
                                  + "      \"name\": \"b\",\n"
                                  + "      \"type\": \"boolean\"\n"
                                  + "    }\n"
                                  + "  ]\n"
                                  + "}")
                          .build())
                  .build())
          .build();

  @Test
  public void testFromDataCatalog() {
    assertEquals(TEST_SCHEMA, SchemaUtils.fromDataCatalog(TEST_DC_SCHEMA));
  }

  @Test
  public void testFromDataCatalogPhysicalSchema() {
    assertEquals(TEST_SCHEMA2, SchemaUtils.fromDataCatalog(TEST_DC_SCHEMA2));
  }

  @Test
  public void testToDataCatalog() {
    assertEquals(TEST_DC_SCHEMA, SchemaUtils.toDataCatalog(TEST_SCHEMA));
  }
}
