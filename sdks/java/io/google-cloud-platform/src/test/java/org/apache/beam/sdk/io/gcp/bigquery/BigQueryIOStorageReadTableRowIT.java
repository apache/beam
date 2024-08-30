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

import static org.apache.beam.sdk.io.gcp.bigquery.TestBigQueryOptions.BIGQUERY_EARLY_ROLLOUT_REGION;

import com.google.api.services.bigquery.model.TableRow;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
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
 * Integration tests for {@link BigQueryIO#readTableRows()} using {@link Method#DIRECT_READ} in
 * combination with {@link TableRowParser} to generate output in {@link TableRow} form.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageReadTableRowIT {

  private static final String DATASET_ID =
      TestPipeline.testingPipelineOptions()
              .as(TestBigQueryOptions.class)
              .getBigQueryLocation()
              .equals(BIGQUERY_EARLY_ROLLOUT_REGION)
          ? "big_query_import_export_day0"
          : "big_query_import_export";
  private static final String TABLE_PREFIX = "parallel_read_table_row_";

  private BigQueryIOStorageReadTableRowOptions options;

  /** Private pipeline options for the test. */
  public interface BigQueryIOStorageReadTableRowOptions
      extends TestPipelineOptions, ExperimentalOptions {
    @Description("The table to be read")
    @Validation.Required
    String getInputTable();

    void setInputTable(String table);
  }

  private static class TableRowToKVPairFn extends SimpleFunction<TableRow, KV<Integer, String>> {
    @Override
    public KV<Integer, String> apply(TableRow input) {
      Integer rowId = Integer.parseInt((String) input.get("id"));
      return KV.of(rowId, BigQueryHelpers.toJsonString(input));
    }
  }

  private void setUpTestEnvironment(String tableName) {
    PipelineOptionsFactory.register(BigQueryIOStorageReadTableRowOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOStorageReadTableRowOptions.class);
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    options.setInputTable(project + ":" + DATASET_ID + "." + TABLE_PREFIX + tableName);
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "temp-it").toString());
  }

  private static void runPipeline(BigQueryIOStorageReadTableRowOptions pipelineOptions) {
    Pipeline pipeline = Pipeline.create(pipelineOptions);

    PCollection<KV<Integer, String>> jsonTableRowsFromExport =
        pipeline
            .apply(
                "ExportTable",
                BigQueryIO.readTableRows()
                    .from(pipelineOptions.getInputTable())
                    .withMethod(Method.EXPORT))
            .apply("MapExportedRows", MapElements.via(new TableRowToKVPairFn()));

    PCollection<KV<Integer, String>> jsonTableRowsFromDirectRead =
        pipeline
            .apply(
                "DirectReadTable",
                BigQueryIO.readTableRows()
                    .from(pipelineOptions.getInputTable())
                    .withMethod(Method.DIRECT_READ))
            .apply("MapDirectReadRows", MapElements.via(new TableRowToKVPairFn()));

    final TupleTag<String> exportTag = new TupleTag<>();
    final TupleTag<String> directReadTag = new TupleTag<>();

    PCollection<KV<Integer, Set<String>>> unmatchedRows =
        KeyedPCollectionTuple.of(exportTag, jsonTableRowsFromExport)
            .and(directReadTag, jsonTableRowsFromDirectRead)
            .apply(CoGroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<Integer, CoGbkResult>, KV<Integer, Set<String>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<Integer, CoGbkResult> element = c.element();

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
  public void testBigQueryStorageReadTableRow100() {
    setUpTestEnvironment("100");
    runPipeline(options);
  }

  @Test
  public void testBigQueryStorageReadTableRow1k() {
    setUpTestEnvironment("1K");
    runPipeline(options);
  }

  @Test
  public void testBigQueryStorageReadTableRow10k() {
    setUpTestEnvironment("10K");
    runPipeline(options);
  }
}
