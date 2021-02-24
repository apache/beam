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

// [START bigquery_read_from_query_with_bigquery_storage_api]

import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

class BigQueryReadFromQueryWithBigQueryStorageAPI {
  public static PCollection<MyData> readFromQueryWithBigQueryStorageAPI(
      String project, String dataset, String table, String query, Pipeline pipeline) {

    // String project = "my-project-id";
    // String dataset = "my_bigquery_dataset_id";
    // String table = "my_bigquery_table_id";

    // Pipeline pipeline = Pipeline.create();

    /*
    String query = String.format("SELECT\n" +
        "  string_field,\n" +
        "  int64_field,\n" +
        "  float64_field,\n" +
        "  numeric_field,\n" +
        "  bool_field,\n" +
        "  bytes_field,\n" +
        "  date_field,\n" +
        "  datetime_field,\n" +
        "  time_field,\n" +
        "  timestamp_field,\n" +
        "  geography_field,\n" +
        "  array_field,\n" +
        "  struct_field\n" +
        "FROM\n" +
        "  `%s:%s.%s`", project, dataset, table)
    */

    PCollection<MyData> rows =
        pipeline
            .apply(
                "Read from BigQuery table",
                BigQueryIO.readTableRows()
                    .fromQuery(query)
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ))
            .apply(
                "TableRows to MyData",
                MapElements.into(TypeDescriptor.of(MyData.class)).via(MyData::fromTableRow));

    return rows;
  }
}
// [END bigquery_read_from_query_with_bigquery_storage_api]
