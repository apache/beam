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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BigQueryIOPushDownIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOPushDownIT.class);
  private static final String NAMESPACE = BigQueryIOPushDownIT.class.getName();
  private static final String TEST_ID = UUID.randomUUID().toString();
  private static final String TEST_TIMESTAMP = Timestamp.now().toString();
  private static final String READ_TIME_METRIC_NAME = "read_time";
  private static final String WRITE_TIME_METRIC_NAME = "write_time";
  private static final String AVRO_WRITE_TIME_METRIC_NAME = "avro_write_time";
  private static String metricsBigQueryTable;
  private static String metricsBigQueryDataset;
  private static String testBigQueryDataset;
  private static String testBigQueryTable;
  private static String tableQualifier;
  private static String tempRoot;
  private static SQLBigQueryPerfTestOptions options;

  @BeforeClass
  public static void setup() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(SQLBigQueryPerfTestOptions.class);
    tempRoot = options.getTempRoot();
    metricsBigQueryDataset = options.getMetricsBigQueryDataset();
    metricsBigQueryTable = options.getMetricsBigQueryTable();
    testBigQueryDataset = options.getTestBigQueryDataset();
    testBigQueryTable = String.format("%s_%s", options.getTestBigQueryTable(), TEST_ID);
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
  public void test() {
    LOG.warn("\n\n\n*** RUNNING SQL PERFORMANCE TESTS ***\n\n\n");
    LOG.warn("With following pipeline option: " + options.toString());
  }

  /** Options for this io performance test. */
  public interface SQLBigQueryPerfTestOptions extends IOTestPipelineOptions {
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

    void setWriteMethod(String value);
  }
}
