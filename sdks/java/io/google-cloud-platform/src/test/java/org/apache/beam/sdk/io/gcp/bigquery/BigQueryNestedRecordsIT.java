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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** An example that exports nested BigQuery record to a file. */
@RunWith(JUnit4.class)
public class BigQueryNestedRecordsIT {
  private static final String RECORD_QUERY =
      "SELECT city.* FROM [apache-beam-testing:big_query_nested_test.source_table]";

  private static final String UNFLATTENABLE_QUERY =
      "SELECT * FROM [apache-beam-testing:big_query_nested_test.genomics_2]";

  /** Options supported by this class. */
  public interface Options extends PipelineOptions {

    @Description("Query for the pipeline input.  Must return exactly one result")
    @Default.String(RECORD_QUERY)
    String getInput();

    void setInput(String value);

    @Description("Query for unflattenable input.  Must return exactly one result")
    @Default.String(UNFLATTENABLE_QUERY)
    String getUnflattenableInput();

    void setunflattenableInput(String value);
  }

  @Test
  public void testNestedRecords() throws Exception {
    PipelineOptionsFactory.register(Options.class);
    TestPipelineOptions testOptions =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    Options options = testOptions.as(Options.class);
    options.setTempLocation(testOptions.getTempRoot() + "/temp-it/");
    runPipeline(options);
  }

  private static void runPipeline(Options options) throws Exception {
    // Create flattened and unflattened collections via Dataflow, via normal and side input
    // paths.
    Pipeline p = Pipeline.create(options);
    BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);

    PCollection<TableRow> flattenedCollection =
        p.apply("ReadFlattened", BigQueryIO.readTableRows().fromQuery(options.getInput()));
    PCollection<TableRow> nonFlattenedCollection =
        p.apply(
            "ReadNonFlattened",
            BigQueryIO.readTableRows().fromQuery(options.getInput()).withoutResultFlattening());
    PCollection<TableRow> unflattenableCollection =
        p.apply(
            "ReadUnflattenable",
            BigQueryIO.readTableRows()
                .fromQuery(options.getUnflattenableInput())
                .withoutResultFlattening());

    // Also query BigQuery directly.
    BigqueryClient bigQueryClient = new BigqueryClient(bigQueryOptions.getAppName());

    TableRow queryFlattenedTyped =
        bigQueryClient
            .queryWithRetries(options.getInput(), bigQueryOptions.getProject(), true)
            .getRows()
            .get(0);

    TableRow queryUnflattened =
        bigQueryClient
            .queryUnflattened(options.getInput(), bigQueryOptions.getProject(), true, false)
            .get(0);

    TableRow queryUnflattenable =
        bigQueryClient
            .queryUnflattened(
                options.getUnflattenableInput(), bigQueryOptions.getProject(), true, false)
            .get(0);

    // Verify that the results are the same.
    PAssert.thatSingleton(flattenedCollection).isEqualTo(queryFlattenedTyped);
    PAssert.thatSingleton(nonFlattenedCollection).isEqualTo(queryUnflattened);
    PAssert.thatSingleton(unflattenableCollection).isEqualTo(queryUnflattenable);

    PAssert.thatSingleton(flattenedCollection).notEqualTo(queryUnflattened);
    p.run().waitUntilFinish();
  }
}
