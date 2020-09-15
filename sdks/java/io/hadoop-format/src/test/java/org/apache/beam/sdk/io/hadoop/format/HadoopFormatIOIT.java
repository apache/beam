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
package org.apache.beam.sdk.io.hadoop.format;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;

import com.google.cloud.Timestamp;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO} on an independent postgres
 * instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/hadoop-format/
 *   -DintegrationTestPipelineOptions='[
 *     "--postgresServerName=1.2.3.4",
 *     "--postgresUsername=postgres",
 *     "--postgresDatabaseName=myfancydb",
 *     "--postgresPassword=mypass",
 *     "--postgresSsl=false",
 *     "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.hadoop.format.HadoopFormatIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class HadoopFormatIOIT {

  private static final String NAMESPACE = HadoopFormatIOIT.class.getName();

  private static PGSimpleDataSource dataSource;
  private static Integer numberOfRows;
  private static String tableName;
  private static SerializableConfiguration hadoopConfiguration;
  private static String bigQueryDataset;
  private static String bigQueryTable;
  private static InfluxDBSettings settings;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    PostgresIOTestPipelineOptions options =
        readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    numberOfRows = options.getNumberOfRecords();
    tableName = DatabaseTestHelper.getTestTableName("HadoopFormatIOIT");
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();

    executeWithRetry(HadoopFormatIOIT::createTable);
    setupHadoopConfiguration(options);
  }

  private static void createTable() throws SQLException {
    DatabaseTestHelper.createTable(dataSource, tableName);
  }

  private static void setupHadoopConfiguration(PostgresIOTestPipelineOptions options) {
    Configuration conf = new Configuration();
    DBConfiguration.configureDB(
        conf,
        "org.postgresql.Driver",
        DatabaseTestHelper.getPostgresDBUrl(options),
        options.getPostgresUsername(),
        options.getPostgresPassword());

    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
    conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, "id", "name");
    conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, "id ASC");
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, TestRowDBWritable.class, DBWritable.class);

    conf.setClass("key.class", LongWritable.class, Object.class);
    conf.setClass("value.class", TestRowDBWritable.class, Object.class);
    conf.setClass("mapreduce.job.inputformat.class", DBInputFormat.class, InputFormat.class);

    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    conf.set(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, "2");
    conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, "id", "name");

    conf.setClass(HadoopFormatIO.OUTPUT_KEY_CLASS, TestRowDBWritable.class, Object.class);
    conf.setClass(HadoopFormatIO.OUTPUT_VALUE_CLASS, NullWritable.class, Object.class);
    conf.setClass(
        HadoopFormatIO.OUTPUT_FORMAT_CLASS_ATTR, DBOutputFormat.class, OutputFormat.class);
    conf.set(HadoopFormatIO.JOB_ID, String.valueOf(1));

    hadoopConfiguration = new SerializableConfiguration(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(HadoopFormatIOIT::deleteTable);
  }

  private static void deleteTable() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
  }

  @Test
  public void writeAndReadUsingHadoopFormat() {
    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRows))
        .apply("Produce db rows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Prevent fusion before writing", Reshuffle.viaRandomKey())
        .apply("Collect write time", ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
        .apply("Construct rows for DBOutputFormat", ParDo.of(new ConstructDBOutputFormatRowFn()))
        .apply(
            "Write using Hadoop OutputFormat",
            HadoopFormatIO.<TestRowDBWritable, NullWritable>write()
                .withConfiguration(hadoopConfiguration.get())
                .withPartitioning()
                .withExternalSynchronization(
                    new HDFSSynchronization(tmpFolder.getRoot().getAbsolutePath())));

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(
                "Read using Hadoop InputFormat",
                HadoopFormatIO.<LongWritable, TestRowDBWritable>read()
                    .withConfiguration(hadoopConfiguration.get()))
            .apply("Collect read time", ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")))
            .apply("Get values only", Values.create())
            .apply("Values as string", ParDo.of(new TestRow.SelectNameFn()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(getExpectedHashForRowCount(numberOfRows));

    PipelineResult readResult = readPipeline.run();
    readResult.waitUntilFinish();

    collectAndPublishMetrics(writeResult, readResult);
  }

  private void collectAndPublishMetrics(PipelineResult writeResult, PipelineResult readResult) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Timestamp.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> readSuppliers = getReadSuppliers(uuid, timestamp);
    Set<Function<MetricsReader, NamedTestResult>> writeSuppliers =
        getWriteSuppliers(uuid, timestamp);

    IOITMetrics readMetrics =
        new IOITMetrics(readSuppliers, readResult, NAMESPACE, uuid, timestamp);
    IOITMetrics writeMetrics =
        new IOITMetrics(writeSuppliers, writeResult, NAMESPACE, uuid, timestamp);
    readMetrics.publish(bigQueryDataset, bigQueryTable);
    readMetrics.publishToInflux(settings);
    writeMetrics.publish(bigQueryDataset, bigQueryTable);
    writeMetrics.publishToInflux(settings);
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

  private Set<Function<MetricsReader, NamedTestResult>> getReadSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, "read_time"));
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
   * Uses the input {@link TestRow} values as seeds to produce new {@link KV}s for {@link
   * HadoopFormatIO}.
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
