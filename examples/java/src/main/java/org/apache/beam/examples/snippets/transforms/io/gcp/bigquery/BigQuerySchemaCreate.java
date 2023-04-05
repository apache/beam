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
package org.apache.beam.examples.snippets.transforms.io.gcp.bigquery;

// [START bigquery_schema_create]

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.Arrays;

class BigQuerySchemaCreate {
  public static TableSchema createSchema() {
    // To learn more about BigQuery schemas:
    // https://cloud.google.com/bigquery/docs/schemas
    TableSchema schema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema()
                        .setName("string_field")
                        .setType("STRING")
                        .setMode("REQUIRED"),
                    new TableFieldSchema()
                        .setName("int64_field")
                        .setType("INT64")
                        .setMode("NULLABLE"),
                    new TableFieldSchema()
                        .setName("float64_field")
                        .setType("FLOAT64"), // default mode is "NULLABLE"
                    new TableFieldSchema().setName("numeric_field").setType("NUMERIC"),
                    new TableFieldSchema().setName("bool_field").setType("BOOL"),
                    new TableFieldSchema().setName("bytes_field").setType("BYTES"),
                    new TableFieldSchema().setName("date_field").setType("DATE"),
                    new TableFieldSchema().setName("datetime_field").setType("DATETIME"),
                    new TableFieldSchema().setName("time_field").setType("TIME"),
                    new TableFieldSchema().setName("timestamp_field").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("geography_field").setType("GEOGRAPHY"),
                    new TableFieldSchema()
                        .setName("array_field")
                        .setType("INT64")
                        .setMode("REPEATED")
                        .setDescription("Setting the mode to REPEATED makes this an ARRAY<INT64>."),
                    new TableFieldSchema()
                        .setName("struct_field")
                        .setType("STRUCT")
                        .setDescription(
                            "A STRUCT accepts a custom data class, the fields must match the custom class fields.")
                        .setFields(
                            Arrays.asList(
                                new TableFieldSchema().setName("string_value").setType("STRING"),
                                new TableFieldSchema().setName("int64_value").setType("INT64")))));
    return schema;
  }
}
// [END bigquery_schema_create]
