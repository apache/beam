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
package org.apache.beam.sdk.io.cdap;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.junit.Assert.assertNotEquals;

import io.cdap.plugin.common.Constants;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * IO Integration test for {@link org.apache.beam.sdk.io.cdap.CdapIO}.
 *
 * <p>{@see https://beam.apache.org/documentation/io/testing/#i-o-transform-integration-tests} for
 * more details.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class CdapIOIT {

  private static final String NAMESPACE = CdapIOIT.class.getName();
  private static final String[] TEST_FIELD_NAMES = new String[] {"id", "name"};
  private static final String TEST_ORDER_BY = "id ASC";

  private static PGSimpleDataSource dataSource;
  private static Integer numberOfRows;
  private static String tableName;
  private static InfluxDBSettings settings;
  private static CdapIOITOptions options;
  private static PostgreSQLContainer postgreSQLContainer;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    options = readIOTestPipelineOptions(CdapIOITOptions.class);
    if (options.isWithTestcontainers()) {
      setPostgresContainer();
    }

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    numberOfRows = options.getNumberOfRecords();
    tableName = DatabaseTestHelper.getTestTableName("CdapIOIT");
    if (!options.isWithTestcontainers()) {
      settings =
          InfluxDBSettings.builder()
              .withHost(options.getInfluxHost())
              .withDatabase(options.getInfluxDatabase())
              .withMeasurement(options.getInfluxMeasurement())
              .get();
    }
    executeWithRetry(CdapIOIT::createTable);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(CdapIOIT::deleteTable);
    if (postgreSQLContainer != null) {
      postgreSQLContainer.stop();
    }
  }

  @Test
  public void testCdapIOReadsAndWritesCorrectlyInBatch() {

    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRows))
        .apply("Produce db rows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Prevent fusion before writing", Reshuffle.viaRandomKey())
        .apply("Collect write time", ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
        .apply("Construct rows for DBOutputFormat", ParDo.of(new ConstructDBOutputFormatRowFn()))
        .apply("Write using CdapIO", writeToDB(getWriteTestParamsFromOptions(options)));

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply("Read using CdapIO", readFromDB(getReadTestParamsFromOptions(options)))
            .apply("Collect read time", ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")))
            .apply("Get values only", Values.create())
            .apply("Values as string", ParDo.of(new TestRow.SelectNameFn()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(getExpectedHashForRowCount(numberOfRows));

    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState = readResult.waitUntilFinish();

    if (!options.isWithTestcontainers()) {
      collectAndPublishMetrics(writeResult, readResult);
    }
    // Fail the test if pipeline failed.
    assertNotEquals(readState, PipelineResult.State.FAILED);
  }

  private CdapIO.Write<TestRowDBWritable, NullWritable> writeToDB(Map<String, Object> params) {
    DBConfig pluginConfig = new ConfigWrapper<>(DBConfig.class).withParams(params).build();

    return CdapIO.<TestRowDBWritable, NullWritable>write()
        .withCdapPlugin(
            Plugin.createBatch(
                DBBatchSink.class, DBOutputFormat.class, DBOutputFormatProvider.class))
        .withPluginConfig(pluginConfig)
        .withKeyClass(TestRowDBWritable.class)
        .withValueClass(NullWritable.class)
        .withLocksDirPath(tmpFolder.getRoot().getAbsolutePath());
  }

  private CdapIO.Read<LongWritable, TestRowDBWritable> readFromDB(Map<String, Object> params) {
    DBConfig pluginConfig = new ConfigWrapper<>(DBConfig.class).withParams(params).build();

    return CdapIO.<LongWritable, TestRowDBWritable>read()
        .withCdapPlugin(
            Plugin.createBatch(
                DBBatchSource.class, DBInputFormat.class, DBInputFormatProvider.class))
        .withPluginConfig(pluginConfig)
        .withKeyClass(LongWritable.class)
        .withValueClass(TestRowDBWritable.class);
  }

  private Map<String, Object> getTestParamsFromOptions(CdapIOITOptions options) {
    Map<String, Object> params = new HashMap<>();
    params.put(DBConfig.DB_URL, DatabaseTestHelper.getPostgresDBUrl(options));
    params.put(DBConfig.POSTGRES_USERNAME, options.getPostgresUsername());
    params.put(DBConfig.POSTGRES_PASSWORD, options.getPostgresPassword());
    params.put(DBConfig.FIELD_NAMES, StringUtils.arrayToString(TEST_FIELD_NAMES));
    params.put(DBConfig.TABLE_NAME, tableName);
    params.put(Constants.Reference.REFERENCE_NAME, "referenceName");
    return params;
  }

  private Map<String, Object> getReadTestParamsFromOptions(CdapIOITOptions options) {
    Map<String, Object> params = getTestParamsFromOptions(options);
    params.put(DBConfig.ORDER_BY, TEST_ORDER_BY);
    params.put(DBConfig.VALUE_CLASS_NAME, TestRowDBWritable.class.getName());
    return params;
  }

  private Map<String, Object> getWriteTestParamsFromOptions(CdapIOITOptions options) {
    Map<String, Object> params = getTestParamsFromOptions(options);
    params.put(DBConfig.FIELD_COUNT, String.valueOf(TEST_FIELD_NAMES.length));
    return params;
  }

  /** Pipeline options specific for this test. */
  public interface CdapIOITOptions extends PostgresIOTestPipelineOptions {

    @Description("Whether to use testcontainers")
    @Default.Boolean(false)
    Boolean isWithTestcontainers();

    void setWithTestcontainers(Boolean withTestcontainers);
  }

  private static void setPostgresContainer() {
    postgreSQLContainer =
        new PostgreSQLContainer(DockerImageName.parse("postgres").withTag("latest"))
            .withDatabaseName(options.getPostgresDatabaseName())
            .withUsername(options.getPostgresUsername())
            .withPassword(options.getPostgresPassword());
    postgreSQLContainer.start();
    options.setPostgresServerName(postgreSQLContainer.getContainerIpAddress());
    options.setPostgresPort(postgreSQLContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setPostgresSsl(false);
  }

  private static void createTable() throws SQLException {
    DatabaseTestHelper.createTable(dataSource, tableName);
  }

  private static void deleteTable() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
  }

  private void collectAndPublishMetrics(PipelineResult writeResult, PipelineResult readResult) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Instant.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> readSuppliers = getReadSuppliers(uuid, timestamp);
    Set<Function<MetricsReader, NamedTestResult>> writeSuppliers =
        getWriteSuppliers(uuid, timestamp);

    IOITMetrics readMetrics =
        new IOITMetrics(readSuppliers, readResult, NAMESPACE, uuid, timestamp);
    IOITMetrics writeMetrics =
        new IOITMetrics(writeSuppliers, writeResult, NAMESPACE, uuid, timestamp);
    readMetrics.publishToInflux(settings);
    writeMetrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> getReadSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, "read_time"));
    return suppliers;
  }

  private Set<Function<MetricsReader, NamedTestResult>> getWriteSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, "write_time"));
    suppliers.add(
        reader ->
            NamedTestResult.create(
                uuid,
                timestamp,
                "data_size",
                DatabaseTestHelper.getPostgresTableSize(dataSource, tableName)
                    .orElseThrow(() -> new IllegalStateException("Unable to fetch table size"))));
    return suppliers;
  }

  private Function<MetricsReader, NamedTestResult> getTimeMetric(
      final String uuid, final String timestamp, final String metricName) {
    return reader -> {
      long startTime = reader.getStartTimeMetric(metricName);
      long endTime = reader.getEndTimeMetric(metricName);
      return NamedTestResult.create(uuid, timestamp, metricName, (endTime - startTime) / 1e3);
    };
  }

  /**
   * Uses the input {@link TestRow} values as seeds to produce new {@link KV}s for {@link CdapIO}.
   */
  static class ConstructDBOutputFormatRowFn
      extends DoFn<TestRow, KV<TestRowDBWritable, NullWritable>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(
          KV.of(new TestRowDBWritable(c.element().id(), c.element().name()), NullWritable.get()));
    }
  }
}
