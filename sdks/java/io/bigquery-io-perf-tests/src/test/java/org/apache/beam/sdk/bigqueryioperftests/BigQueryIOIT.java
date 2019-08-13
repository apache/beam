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
package org.apache.beam.sdk.bigqueryioperftests;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A performance test of {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}.
 *
 * <p>Usage:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/gcp/bigquery -DintegrationTestPipelineOptions='[
 *  "--testBigQueryDataset=test-dataset",
 *  "--testBigQueryTable=test-table",
 *  "--metricsBigQueryDataset=metrics-dataset",
 *  "--metricsBigQueryTable=metrics-table",
 *  "--writeMethod=FILE_LOADS",
 *  "--sourceOptions={"numRecords":"1000", "keySize":1, valueSize:"1024"}
 *  }"]'
 *  --tests org.apache.beam.sdk.io.gcp.bigQuery.BigQueryIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class BigQueryIOIT {
  private static final String NAMESPACE = BigQueryIOIT.class.getName();
  private static final String TEST_ID = UUID.randomUUID().toString();
  private static final String TEST_TIMESTAMP = Timestamp.now().toString();
  private static final String READ_TIME_METRIC_NAME = "read_time";
  private static final String WRITE_TIME_METRIC_NAME = "write_time";
  private static String metricsBigQueryTable;
  private static String metricsBigQueryDataset;
  private static String testBigQueryDataset;
  private static String testBigQueryTable;
  private static SyntheticSourceOptions sourceOptions;
  private static String tableQualifier;
  private static String tempRoot;
  private static BigQueryPerfTestOptions options;

  @BeforeClass
  public static void setup() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(BigQueryPerfTestOptions.class);
    tempRoot = options.getTempRoot();
    sourceOptions =
        SyntheticOptions.fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
    metricsBigQueryDataset = options.getMetricsBigQueryDataset();
    metricsBigQueryTable = options.getMetricsBigQueryTable();
    testBigQueryDataset = options.getTestBigQueryDataset();
    testBigQueryTable = options.getTestBigQueryTable();
    BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder().build();
    tableQualifier =
        String.format(
            "%s:%s.%s", bigQueryOptions.getProjectId(), testBigQueryDataset, testBigQueryTable);
  }

  @AfterClass
  public static void tearDown() {
    BigQueryOptions options = BigQueryOptions.newBuilder().build();
    BigQuery client = options.getService();
    TableId tableId = TableId.of(options.getProjectId(), testBigQueryDataset, testBigQueryTable);
    client.delete(tableId);
  }

  @Test
  public void testWriteThenRead() {
    testWrite();
    testRead();
  }

  private void testWrite() {
    Pipeline pipeline = Pipeline.create(options);

    BigQueryIO.Write.Method method = BigQueryIO.Write.Method.valueOf(options.getWriteMethod());
    pipeline
        .apply("Read from source", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Gather time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply("Map records", ParDo.of(new MapKVToV()))
        .apply(
            "Write to BQ",
            BigQueryIO.<byte[]>write()
                .to(tableQualifier)
                .withFormatFunction(
                    input -> {
                      TableRow tableRow = new TableRow();
                      tableRow.set("data", input);
                      return tableRow;
                    })
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempRoot))
                .withMethod(method)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("data").setType("BYTES")))));

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    extractAndPublishTime(pipelineResult, WRITE_TIME_METRIC_NAME);
  }

  private void testRead() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Read from BQ", BigQueryIO.readTableRows().from(tableQualifier))
        .apply("Gather time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    extractAndPublishTime(result, READ_TIME_METRIC_NAME);
  }

  private void extractAndPublishTime(PipelineResult pipelineResult, String writeTimeMetricName) {
    NamedTestResult metricResult =
        getMetricSupplier(writeTimeMetricName).apply(new MetricsReader(pipelineResult, NAMESPACE));
    IOITMetrics.publish(
        TEST_ID,
        TEST_TIMESTAMP,
        metricsBigQueryDataset,
        metricsBigQueryTable,
        Collections.singletonList(metricResult));
  }

  private static Function<MetricsReader, NamedTestResult> getMetricSupplier(String metricName) {
    return reader -> {
      long startTime = reader.getStartTimeMetric(metricName);
      long endTime = reader.getEndTimeMetric(metricName);
      return NamedTestResult.create(
          TEST_ID, TEST_TIMESTAMP, metricName, (endTime - startTime) / 1e3);
    };
  }

  /** Options for this io performance test. */
  public interface BigQueryPerfTestOptions extends IOTestPipelineOptions {
    @Description("Synthetic source options")
    @Validation.Required
    String getSourceOptions();

    void setSourceOptions(String value);

    @Description("BQ dataset for the test data")
    String getTestBigQueryDataset();

    void setTestBigQueryDataset(String dataset);

    @Description("BQ table for test data")
    String getTestBigQueryTable();

    void setTestBigQueryTable(String table);

    @Description("BQ dataset for the metrics data")
    String getMetricsBigQueryDataset();

    void setMetricsBigQueryDataset(String dataset);

    @Description("BQ table for metrics data")
    String getMetricsBigQueryTable();

    void setMetricsBigQueryTable(String table);

    @Description("Should test use streaming writes or batch loads to BQ")
    String getWriteMethod();

    void setWriteMethod(Boolean value);
  }

  private static class MapKVToV extends DoFn<KV<byte[], byte[]>, byte[]> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(context.element().getValue());
    }
  }
}
