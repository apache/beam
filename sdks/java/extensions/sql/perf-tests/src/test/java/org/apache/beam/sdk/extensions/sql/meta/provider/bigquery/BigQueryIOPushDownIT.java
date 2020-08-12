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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryIOPushDownIT {
  private static final String READ_FROM_TABLE =
      "apache-beam-testing:beam_performance.hacker_news_full";
  private static final String NAMESPACE = BigQueryIOPushDownIT.class.getName();
  private static final String FIELDS_READ_METRIC = "fields_read";
  private static final String READ_TIME_METRIC = "read_time";
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
      "SELECT `by` as author, type, title, score from HACKER_NEWS where (type='story' or type='job') and score>2";

  //  https://github.com/typetools/checker-framework/issues/1525
  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static SQLBigQueryPerfTestOptions options;

  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static String metricsBigQueryDataset;

  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static String metricsBigQueryTable;

  @SuppressWarnings("initialization.static.fields.uninitialized")
  private static InfluxDBSettings settings;

  private Pipeline pipeline = Pipeline.create(options);

  @SuppressWarnings("initialization.fields.uninitialized")
  private BeamSqlEnv sqlEnv;

  @BeforeClass
  public static void setUp() {
    options = IOITHelper.readIOTestPipelineOptions(SQLBigQueryPerfTestOptions.class);
    metricsBigQueryDataset = options.getMetricsBigQueryDataset();
    metricsBigQueryTable = options.getMetricsBigQueryTable();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Before
  public void before() {
    sqlEnv = BeamSqlEnv.inMemory(new BigQueryPerfTableProvider(NAMESPACE, FIELDS_READ_METRIC));
  }

  @Test
  public void readUsingDirectReadMethodPushDown() {
    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, Method.DIRECT_READ.toString()));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(pipeline, beamRelNode)
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result, "_directread_pushdown");
  }

  @Test
  public void readUsingDirectReadMethod() {
    List<RelOptRule> ruleList = new ArrayList<>();
    for (RuleSet x : getRuleSets()) {
      x.iterator().forEachRemaining(ruleList::add);
    }
    // Remove push-down rule
    ruleList.remove(BeamIOPushDownRule.INSTANCE);

    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    inMemoryMetaStore.registerProvider(
        new BigQueryPerfTableProvider(NAMESPACE, FIELDS_READ_METRIC));
    sqlEnv =
        BeamSqlEnv.builder(inMemoryMetaStore)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(ImmutableList.of(RuleSets.ofList(ruleList)))
            .build();
    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, Method.DIRECT_READ.toString()));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(pipeline, beamRelNode)
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result, "_directread");
  }

  @Test
  public void readUsingDefaultMethod() {
    sqlEnv.executeDdl(String.format(CREATE_TABLE_STATEMENT, Method.DEFAULT.toString()));

    BeamRelNode beamRelNode = sqlEnv.parseQuery(SELECT_STATEMENT);
    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(pipeline, beamRelNode)
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC)));

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
    readMetrics.publishToInflux(settings.copyWithMeasurement(settings.measurement + postfix));
  }

  private Set<Function<MetricsReader, NamedTestResult>> getReadSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long readStart = reader.getStartTimeMetric(READ_TIME_METRIC);
          long readEnd = reader.getEndTimeMetric(READ_TIME_METRIC);
          return NamedTestResult.create(
              uuid, timestamp, READ_TIME_METRIC, (readEnd - readStart) / 1e3);
        });
    suppliers.add(
        reader -> {
          long fieldsRead = reader.getCounterMetric(FIELDS_READ_METRIC);
          return NamedTestResult.create(uuid, timestamp, FIELDS_READ_METRIC, fieldsRead);
        });
    return suppliers;
  }

  /** Options for this io performance test. */
  public interface SQLBigQueryPerfTestOptions extends IOTestPipelineOptions {
    @Description("BQ dataset for the metrics data")
    String getMetricsBigQueryDataset();

    void setMetricsBigQueryDataset(String dataset);

    @Description("BQ table for metrics data")
    String getMetricsBigQueryTable();

    void setMetricsBigQueryTable(String table);
  }
}
