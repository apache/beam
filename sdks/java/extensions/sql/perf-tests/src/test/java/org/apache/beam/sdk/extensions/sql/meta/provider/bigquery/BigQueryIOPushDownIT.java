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

import static org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets.getRuleSets;

import com.google.cloud.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOPushDownRule;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
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
          + "LOCATION '"
          + READ_FROM_TABLE
          + "' \n"
          + "TBLPROPERTIES '{ method: \"%s\" }'";
  private static final String SELECT_STATEMENT =
      "SELECT `by` as author, type, title, score from HACKER_NEWS where (type='story' or type='job') and score>100";

  private static SQLBigQueryPerfTestOptions options;
  private static String metricsBigQueryDataset;
  private static String metricsBigQueryTable;
  private Pipeline pipeline = Pipeline.create(options);
  private BeamSqlEnv sqlEnv;

  @BeforeClass
  public static void setUp() {
    options = IOITHelper.readIOTestPipelineOptions(SQLBigQueryPerfTestOptions.class);
    metricsBigQueryDataset = options.getMetricsBigQueryDataset();
    metricsBigQueryTable = options.getMetricsBigQueryTable();
  }

  @Before
  public void before() {
    sqlEnv = BeamSqlEnv.inMemory(new BigQueryPerfTableProvider(NAMESPACE, "read_time"));
  }

  @Test
  public void readUsingDirectReadMethodPushDown() {
    LOG.warn("\n\n\n*** RUNNING SQL PERFORMANCE TESTS ***\n\n\n");
    LOG.warn("With following pipeline option: " + options.toString());

    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, "DIRECT_READ"));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result, "_directread_pushdown");
  }

  @Test
  public void readUsingDirectReadMethod() {
    LOG.warn("\n\n\n*** RUNNING SQL PERFORMANCE TESTS ***\n\n\n");
    LOG.warn("With following pipeline option: " + options.toString());

    List<RelOptRule> ruleList = new ArrayList<>();
    for (RuleSet x : getRuleSets()) {
      x.iterator().forEachRemaining(ruleList::add);
    }
    // Remove push-down rule
    ruleList.remove(BeamIOPushDownRule.INSTANCE);

    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    inMemoryMetaStore.registerProvider(new BigQueryPerfTableProvider(NAMESPACE, "read_time"));
    sqlEnv =
        BeamSqlEnv.builder(inMemoryMetaStore)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(new RuleSet[] {RuleSets.ofList(ruleList)})
            .build();
    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, "DIRECT_READ"));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result, "_directread");
  }

  @Test
  public void readUsingDefaultMethod() {
    LOG.warn("\n\n\n*** RUNNING SQL PERFORMANCE TESTS ***\n\n\n");
    LOG.warn("With following pipeline option: " + options.toString());

    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, "DEFAULT"));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result, "_default");
  }

  private void collectAndPublishMetrics(PipelineResult readResult, String postfix) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Timestamp.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> readSuppliers = getReadSuppliers(uuid, timestamp);
    IOITMetrics readMetrics =
        new IOITMetrics(readSuppliers, readResult, NAMESPACE, uuid, timestamp);
    readMetrics.publish(metricsBigQueryDataset, metricsBigQueryTable + postfix);
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
