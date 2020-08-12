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

// [START bigquery_write_to_table]

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.values.PCollection;

class BigQueryWriteToTable {
  public static void writeToTable(
      String project,
      String dataset,
      String table,
      TableSchema schema,
      PCollection<TableRow> rows) {

    // String project = "my-project-id";
    // String dataset = "my_bigquery_dataset_id";
    // String table = "my_bigquery_table_id";

    // TableSchema schema = new TableSchema().setFields(Arrays.asList(...));

    // Pipeline pipeline = Pipeline.create();
    // PCollection<TableRow> rows = ...

    rows.apply(
        "Write to BigQuery",
        BigQueryIO.writeTableRows()
            .to(String.format("%s:%s.%s", project, dataset, table))
            .withSchema(schema)
            // For CreateDisposition:
            // - CREATE_IF_NEEDED (default): creates the table if it doesn't exist, a schema is
            // required
            // - CREATE_NEVER: raises an error if the table doesn't exist, a schema is not needed
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            // For WriteDisposition:
            // - WRITE_EMPTY (default): raises an error if the table is not empty
            // - WRITE_APPEND: appends new rows to existing rows
            // - WRITE_TRUNCATE: deletes the existing rows before writing
            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));

    // pipeline.run().waitUntilFinish();
  }
}
// [END bigquery_write_to_table]
