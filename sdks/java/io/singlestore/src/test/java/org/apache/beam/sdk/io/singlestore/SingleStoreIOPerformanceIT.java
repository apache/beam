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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.junit.Assert.assertEquals;

import com.singlestore.jdbc.SingleStoreDataSource;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleStoreIOPerformanceIT {

  private static final String NAMESPACE = SingleStoreIOPerformanceIT.class.getName();

  private static final String DATABASE_NAME = "SingleStoreIOIT";

  private static int numberOfRows;

  private static String tableName;

  private static String serverName;

  private static String username;

  private static String password;

  private static Integer port;

  private static SingleStoreIO.DataSourceConfiguration dataSourceConfiguration;

  private static InfluxDBSettings settings;

  @BeforeClass
  public static void setup() {
    SingleStoreIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(SingleStoreIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);

    numberOfRows = options.getNumberOfRecords();
    serverName = options.getSingleStoreServerName();
    username = options.getSingleStoreUsername();
    password = options.getSingleStorePassword();
    port = options.getSingleStorePort();
    tableName = DatabaseTestHelper.getTestTableName("IT");
    dataSourceConfiguration =
        SingleStoreIO.DataSourceConfiguration.create(serverName + ":" + port)
            .withDatabase(DATABASE_NAME)
            .withPassword(password)
            .withUsername(username);
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenRead() throws Exception {
    TestHelper.createDatabaseIfNotExists(serverName, port, username, password, DATABASE_NAME);
    DataSource dataSource =
        new SingleStoreDataSource(
            String.format(
                "jdbc:singlestore://%s:%d/%s?user=%s&password=%s&allowLocalInfile=TRUE",
                serverName, port, DATABASE_NAME, username, password));
    DatabaseTestHelper.createTable(dataSource, tableName);
    try {
      PipelineResult writeResult = runWrite();
      assertEquals(PipelineResult.State.DONE, writeResult.waitUntilFinish());
      PipelineResult readResult = runRead();
      assertEquals(PipelineResult.State.DONE, readResult.waitUntilFinish());
      PipelineResult readResultWithPartitions = runReadWithPartitions();
      assertEquals(PipelineResult.State.DONE, readResultWithPartitions.waitUntilFinish());
      gatherAndPublishMetrics(writeResult, readResult, readResultWithPartitions);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  private void gatherAndPublishMetrics(
      PipelineResult writeResult,
      PipelineResult readResult,
      PipelineResult readResultWithPartitions) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Instant.now().toString();

    IOITMetrics writeMetrics =
        new IOITMetrics(
            getMetricSuppliers(uuid, timestamp, "write_time"),
            writeResult,
            NAMESPACE,
            uuid,
            timestamp);
    writeMetrics.publishToInflux(settings);

    IOITMetrics readMetrics =
        new IOITMetrics(
            getMetricSuppliers(uuid, timestamp, "read_time"),
            readResult,
            NAMESPACE,
            uuid,
            timestamp);
    readMetrics.publishToInflux(settings);

    IOITMetrics readMetricsWithPartitions =
        new IOITMetrics(
            getMetricSuppliers(uuid, timestamp, "read_with_partitions_time"),
            readResultWithPartitions,
            NAMESPACE,
            uuid,
            timestamp);
    readMetricsWithPartitions.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> getMetricSuppliers(
      String uuid, String timestamp, String metricName) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();

    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric(metricName);
          long writeEnd = reader.getEndTimeMetric(metricName);
          return NamedTestResult.create(uuid, timestamp, metricName, (writeEnd - writeStart) / 1e3);
        });

    return suppliers;
  }

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();
  @Rule public TestPipeline pipelineReadWithPartitions = TestPipeline.create();

  private PipelineResult runWrite() {
    PCollection<Integer> writtenRows =
        pipelineWrite
            .apply(GenerateSequence.from(0).to(numberOfRows))
            .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
            .apply(
                SingleStoreIO.<TestRow>write()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withTable(tableName)
                    .withUserDataMapper(new TestHelper.TestUserDataMapper()));

    PAssert.thatSingleton(writtenRows.apply("Sum All", Sum.integersGlobally()))
        .isEqualTo(numberOfRows);

    return pipelineWrite.run();
  }

  private PipelineResult runRead() {
    PCollection<TestRow> namesAndIds =
        pipelineRead
            .apply(
                SingleStoreIO.<TestRow>read()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withTable(tableName)
                    .withRowMapper(new TestHelper.TestRowMapper()))
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")));

    testReadResult(namesAndIds);

    return pipelineRead.run();
  }

  private PipelineResult runReadWithPartitions() {
    PCollection<TestRow> namesAndIds =
        pipelineReadWithPartitions
            .apply(
                SingleStoreIO.<TestRow>readWithPartitions()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withTable(tableName)
                    .withRowMapper(new TestHelper.TestRowMapper()))
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "read_with_partitions_time")));

    testReadResult(namesAndIds);

    return pipelineReadWithPartitions.run();
  }

  private void testReadResult(PCollection<TestRow> namesAndIds) {
    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRows);

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRows));

    PCollection<List<TestRow>> frontOfList = namesAndIds.apply(Top.smallest(500));
    Iterable<TestRow> expectedFrontOfList = TestRow.getExpectedValues(0, 500);
    PAssert.thatSingletonIterable(frontOfList).containsInAnyOrder(expectedFrontOfList);

    PCollection<List<TestRow>> backOfList = namesAndIds.apply(Top.largest(500));
    Iterable<TestRow> expectedBackOfList =
        TestRow.getExpectedValues(numberOfRows - 500, numberOfRows);
    PAssert.thatSingletonIterable(backOfList).containsInAnyOrder(expectedBackOfList);
  }
}
