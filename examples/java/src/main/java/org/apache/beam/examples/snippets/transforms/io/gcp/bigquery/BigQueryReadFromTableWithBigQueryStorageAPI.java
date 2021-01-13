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

// [START bigquery_read_from_table_with_bigquery_storage_api]

import java.util.Arrays;
import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

class BigQueryReadFromTableWithBigQueryStorageAPI {
  public static PCollection<MyData> readFromTableWithBigQueryStorageAPI(
      String project, String dataset, String table, Pipeline pipeline) {

    // String project = "my-project-id";
    // String dataset = "my_bigquery_dataset_id";
    // String table = "my_bigquery_table_id";

    // Pipeline pipeline = Pipeline.create();

    PCollection<MyData> rows =
        pipeline
            .apply(
                "Read from BigQuery table",
                BigQueryIO.readTableRows()
                    .from(String.format("%s:%s.%s", project, dataset, table))
                    .withMethod(Method.DIRECT_READ)
                    .withSelectedFields(
                        Arrays.asList(
                            "string_field",
                            "int64_field",
                            "float64_field",
                            "numeric_field",
                            "bool_field",
                            "bytes_field",
                            "date_field",
                            "datetime_field",
                            "time_field",
                            "timestamp_field",
                            "geography_field",
                            "array_field",
                            "struct_field")))
            .apply(
                "TableRows to MyData",
                MapElements.into(TypeDescriptor.of(MyData.class)).via(MyData::fromTableRow));

    return rows;
  }
}
// [END bigquery_read_from_table_with_bigquery_storage_api]
