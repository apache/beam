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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.io.common.TestRow.DeterministicallyConstructTestRowFn;
import static org.apache.beam.sdk.io.common.TestRow.SelectNameFn;
import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.io.hadoop.inputformat.TestRowDBWritable.PrepareStatementFromTestRow;

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
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO} on an independent
 * postgres instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/hadoop-input-format/
 *   -DintegrationTestPipelineOptions='[
 *     "--postgresServerName=1.2.3.4",
 *     "--postgresUsername=postgres",
 *     "--postgresDatabaseName=myfancydb",
 *     "--postgresPassword=mypass",
 *     "--postgresSsl=false",
 *     "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOIT {

  private static final String NAMESPACE = HadoopInputFormatIOIT.class.getName();

  private static PGSimpleDataSource dataSource;
  private static Integer numberOfRows;
  private static String tableName;
  private static SerializableConfiguration hadoopConfiguration;
  private static String bigQueryDataset;
  private static String bigQueryTable;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws Exception {
    PostgresIOTestPipelineOptions options =
        readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    numberOfRows = options.getNumberOfRecords();
    tableName = DatabaseTestHelper.getTestTableName("HadoopInputFormatIOIT");
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();

    executeWithRetry(HadoopInputFormatIOIT::createTable);
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

    hadoopConfiguration = new SerializableConfiguration(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(HadoopInputFormatIOIT::deleteTable);
  }

  private static void deleteTable() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
  }

  @Test
  public void readUsingHadoopInputFormat() {
    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfRows))
        .apply("Produce db rows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply("Prevent fusion before writing", Reshuffle.viaRandomKey())
        .apply("Collect write time", ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
        .apply(
            "Write using JDBCIO",
            JdbcIO.<TestRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withPreparedStatementSetter(new PrepareStatementFromTestRow()));

    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(
                "Read using HadoopInputFormat",
                HadoopInputFormatIO.<LongWritable, TestRowDBWritable>read()
                    .withConfiguration(hadoopConfiguration.get()))
            .apply("Collect read time", ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")))
            .apply("Get values only", Values.create())
            .apply("Values as string", ParDo.of(new SelectNameFn()))
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
    writeMetrics.publish(bigQueryDataset, bigQueryTable);
  }

  private Set<Function<MetricsReader, NamedTestResult>> getWriteSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("write_time");
          long writeEnd = reader.getEndTimeMetric("write_time");
          return NamedTestResult.create(
              uuid, timestamp, "write_time", (writeEnd - writeStart) / 1e3);
        });
    return suppliers;
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
}
