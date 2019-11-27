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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BigQueryIOPushDownIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOPushDownIT.class);
  private static final String READ_FROM_TABLE = "bigquery-public-data:hacker_news.full";
  private static final String NAMESPACE = BigQueryIOPushDownIT.class.getName();
  private static final String CREATE_TABLE_STATEMENT =
      "CREATE EXTERNAL TABLE HACKER_NEWS( \n"
          + "   title VARCHAR, \n"
          + "   url VARCHAR, \n"
          + "   text VARCHAR, \n"
          + "   dead BOOLEAN, \n"
          + "   `by` VARCHAR, \n"
          + "   score INTEGER, \n"
          + "   `time` INTEGER, \n"
          + "   `timestamp` TIMESTAMP, \n"
          + "   type VARCHAR, \n"
          + "   id INTEGER, \n"
          + "   parent INTEGER, \n"
          + "   descendants INTEGER, \n"
          + "   ranking INTEGER, \n"
          + "   deleted BOOLEAN \n"
          + ") \n"
          + "TYPE 'bigquery' \n"
          + "LOCATION '" + READ_FROM_TABLE + "' \n"
          + "TBLPROPERTIES '{ method: \"%s\" }'";
  private static final String SELECT_STATEMENT =
      "SELECT `by` as author, title, score from HACKER_NEWS where type='story' and score>1000";

  private static SQLBigQueryPerfTestOptions options;
  private static String metricsBigQueryDataset;
  private static String metricsBigQueryTable;
  private static String tableQualifier;
  private static String tempRoot;
  Pipeline pipeline = Pipeline.create(options);
  private BeamSqlEnv sqlEnv;

  @BeforeClass
  public static void setUp() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(SQLBigQueryPerfTestOptions.class);
    tempRoot = options.getTempRoot();
    metricsBigQueryDataset = options.getMetricsBigQueryDataset();
    metricsBigQueryTable = options.getMetricsBigQueryTable();
  }

  @Before
  public void before() {
    sqlEnv = BeamSqlEnv.inMemory(new BigQueryPerfTableProvider(NAMESPACE, "read_time"));
  }

  @Test
  public void readUsingDirectReadMethod() {
    LOG.warn("\n\n\n*** RUNNING SQL PERFORMANCE TESTS ***\n\n\n");
    LOG.warn("With following pipeline option: " + options.toString());

    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, "DIRECT_READ"));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    pipeline.run().waitUntilFinish();
  }

  private Set<Function<MetricsReader, NamedTestResult>> getReadSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long readStart = reader.getStartTimeMetric("read_time");
          long readEnd = reader.getEndTimeMetric("read_time");
          return NamedTestResult.create(uuid, timestamp, "read_time", (readEnd - readStart) / 1e3);
        });
    return suppliers;
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
