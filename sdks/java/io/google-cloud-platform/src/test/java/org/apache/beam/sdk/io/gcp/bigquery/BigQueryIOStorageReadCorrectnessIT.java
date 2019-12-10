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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link BigQueryIO#readTableRows()} using {@link Method#EXPORT} and {@link
 * Method#DIRECT_READ} to ensure that both methods return exactly the same data.
 *
 * <p>Note that we use a relatively large table for this test because we want to exercise the
 * sharding of data that the BigQuery Storage API server performs.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadCorrectnessIT {

  private static final String DATASET_ID = "big_query_storage";
  private static final String TABLE_ID = "storage_read_1G";

  private BigQueryIOStorageReadTableRowOptions options;

  /** Private pipeline options for the test. */
  public interface BigQueryIOStorageReadTableRowOptions
      extends TestPipelineOptions, ExperimentalOptions {

    @Description("The table to be read")
    @Validation.Required
    String getInputTable();

    void setInputTable(String table);
  }

  private static class TableRowToKVPairFn extends SimpleFunction<TableRow, KV<String, String>> {

    @Override
    public KV<String, String> apply(TableRow input) {
      CharSequence key = (CharSequence) input.get("string_field");
      if (key == null) {
        throw new NullPointerException(
            "Found row that did not contain a value for field 'string_field'.");
      }
      return KV.of(key.toString(), BigQueryHelpers.toJsonString(input));
    }
  }

  private void setUpTestEnvironment(String tableName) {}

  private static void runPipeline(BigQueryIOStorageReadTableRowOptions pipelineOptions) {
    Pipeline pipeline = Pipeline.create(pipelineOptions);

    PCollection<KV<String, String>> jsonTableRowsFromExport =
        pipeline
            .apply(
                "ExportTable",
                BigQueryIO.readTableRows()
                    .from(pipelineOptions.getInputTable())
                    .withMethod(Method.EXPORT))
            .apply("MapExportedRows", MapElements.via(new TableRowToKVPairFn()));

    PCollection<KV<String, String>> jsonTableRowsFromDirectRead =
        pipeline
            .apply(
                "DirectReadTable",
                BigQueryIO.readTableRows()
                    .from(pipelineOptions.getInputTable())
                    .withMethod(Method.DIRECT_READ))
            .apply("MapDirectReadRows", MapElements.via(new TableRowToKVPairFn()));

    final TupleTag<String> exportTag = new TupleTag<>();
    final TupleTag<String> directReadTag = new TupleTag<>();

    PCollection<KV<String, Set<String>>> unmatchedRows =
        KeyedPCollectionTuple.of(exportTag, jsonTableRowsFromExport)
            .and(directReadTag, jsonTableRowsFromDirectRead)
            .apply(CoGroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, CoGbkResult>, KV<String, Set<String>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        KV<String, CoGbkResult> element = c.element();

                        // Add all the exported rows for the key to a collection.
                        Set<String> uniqueRows = new HashSet<>();
                        for (String row : element.getValue().getAll(exportTag)) {
                          uniqueRows.add(row);
                        }

                        // Compute the disjunctive union of the rows in the direct read collection.
                        for (String row : element.getValue().getAll(directReadTag)) {
                          if (uniqueRows.contains(row)) {
                            uniqueRows.remove(row);
                          } else {
                            uniqueRows.add(row);
                          }
                        }

                        // Emit any rows in the result set.
                        if (!uniqueRows.isEmpty()) {
                          c.output(KV.of(element.getKey(), uniqueRows));
                        }
                      }
                    }));

    PAssert.that(unmatchedRows).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testBigQueryStorageReadCorrectness() throws Exception {
    PipelineOptionsFactory.register(BigQueryIOStorageReadTableRowOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOStorageReadTableRowOptions.class);
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    options.setInputTable(project + ":" + DATASET_ID + "." + TABLE_ID);
    options.setTempLocation(options.getTempRoot() + "/temp-it/");
    runPipeline(options);
  }
}
