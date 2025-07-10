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
package org.apache.beam.sdk.io.jdbc;

import static org.apache.beam.sdk.io.common.DatabaseTestHelper.assertRowCount;
import static org.apache.beam.sdk.io.common.DatabaseTestHelper.getTestDataToWrite;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.junit.Assert.assertNotEquals;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicSequence;
import org.apache.beam.sdk.transforms.PeriodicSequence.SequenceDefinition;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/jdbc -DintegrationTestPipelineOptions='[
 *  "--postgresServerName=1.2.3.4",
 *  "--postgresUsername=postgres",
 *  "--postgresDatabaseName=myfancydb",
 *  "--postgresPassword=mypass",
 *  "--postgresSsl=false",
 *  "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.jdbc.JdbcIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class JdbcIOIT {

  private static final String NAMESPACE = JdbcIOIT.class.getName();
  // the number of rows written to table in the performance test.
  private static int numberOfRows;
  private static PGSimpleDataSource dataSource;
  private static String tableName;
  private static InfluxDBSettings settings;
  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    PostgresIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);
    numberOfRows = options.getNumberOfRecords();

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    tableName = DatabaseTestHelper.getTestTableName("IT");
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  /**
   * Tests writing then reading data for a postgres database. Also used as a performance test of
   * JDBCIO.
   */
  @Test
  public void testWriteThenRead() throws SQLException {
    DatabaseTestHelper.createTable(dataSource, tableName);
    try {
      PipelineResult writeResult = runWrite();
      PipelineResult.State writeState = writeResult.waitUntilFinish();
      PipelineResult readResult = runRead(tableName);
      PipelineResult.State readState = readResult.waitUntilFinish();
      gatherAndPublishMetrics(writeResult, readResult);
      // Fail the test if pipeline failed.
      assertNotEquals(PipelineResult.State.FAILED, writeState);
      assertNotEquals(PipelineResult.State.FAILED, readState);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  private void gatherAndPublishMetrics(PipelineResult writeResult, PipelineResult readResult) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Instant.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        getWriteMetricSuppliers(uuid, timestamp);
    IOITMetrics writeMetrics =
        new IOITMetrics(metricSuppliers, writeResult, NAMESPACE, uuid, timestamp);
    writeMetrics.publishToInflux(settings);

    IOITMetrics readMetrics =
        new IOITMetrics(
            getReadMetricSuppliers(uuid, timestamp), readResult, NAMESPACE, uuid, timestamp);
    readMetrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> getWriteMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    Optional<Long> postgresTableSize =
        DatabaseTestHelper.getPostgresTableSize(dataSource, tableName);

    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("write_time");
          long writeEnd = reader.getEndTimeMetric("write_time");
          return NamedTestResult.create(
              uuid, timestamp, "write_time", (writeEnd - writeStart) / 1e3);
        });

    postgresTableSize.ifPresent(
        tableFinalSize ->
            suppliers.add(
                ignore -> NamedTestResult.create(uuid, timestamp, "total_size", tableFinalSize)));
    return suppliers;
  }

  private Set<Function<MetricsReader, NamedTestResult>> getReadMetricSuppliers(
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

  /**
   * Writes the test dataset to postgres.
   *
   * <p>This method does not attempt to validate the data - we do so in the read test. This does
   * make it harder to tell whether a test failed in the write or read phase, but the tests are much
   * easier to maintain (don't need any separate code to write test data for read tests to the
   * database.)
   */
  private PipelineResult runWrite() {
    pipelineWrite
        .apply(GenerateSequence.from(0).to(numberOfRows))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
        .apply(
            JdbcIO.<TestRow>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withPreparedStatementSetter(new JdbcTestHelper.PrepareStatementFromTestRow()));

    return pipelineWrite.run();
  }

  /**
   * Read the test dataset from postgres and validate its contents.
   *
   * <p>When doing the validation, we wish to ensure that we: 1. Ensure *all* the rows are correct
   * 2. Provide enough information in assertions such that it is easy to spot obvious errors (e.g.
   * all elements have a similar mistake, or "only 5 elements were generated" and the user wants to
   * see what the problem was.
   *
   * <p>We do not wish to generate and compare all of the expected values, so this method uses
   * hashing to ensure that all expected data is present. However, hashing does not provide easy
   * debugging information (failures like "every element was empty string" are hard to see), so we
   * also: 1. Generate expected values for the first and last 500 rows 2. Use containsInAnyOrder to
   * verify that their values are correct. Where first/last 500 rows is determined by the fact that
   * we know all rows have a unique id - we can use the natural ordering of that key.
   */
  private PipelineResult runRead(String tableName) {
    if (tableName == null) {
      tableName = JdbcIOIT.tableName;
    }
    PCollection<TestRow> namesAndIds =
        pipelineRead
            .apply(
                JdbcIO.<TestRow>read()
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                    .withQuery(String.format("select name,id from %s;", tableName))
                    .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId()))
            .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "read_time")));

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

    return pipelineRead.run();
  }

  /** An integration test of auto sharding functionality using test stream. */
  @Test
  public void testWriteWithAutosharding() throws Exception {
    String firstTableName = DatabaseTestHelper.getTestTableName("JDBCIT_AUTOSHARD");
    DatabaseTestHelper.createTable(dataSource, firstTableName);
    try {
      PCollection<TestRow> dataCollection =
          pipelineWrite.apply(
              // emit 50_000 elements per seconds.
              new GenerateRecordsStream(numberOfRows, 50_000, Duration.standardSeconds(1)));
      dataCollection
          .apply(ParDo.of(new TimeMonitor<>(NAMESPACE, "write_time")))
          .apply(
              JdbcIO.<TestRow>write()
                  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                  .withStatement(String.format("insert into %s values(?, ?)", firstTableName))
                  .withAutoSharding()
                  .withPreparedStatementSetter(new JdbcTestHelper.PrepareStatementFromTestRow()));

      List<String> additionalArgs = Lists.newArrayList("--streaming");
      if (pipelineWrite
          .getOptions()
          .getRunner()
          .getCanonicalName()
          .startsWith("org.apache.beam.runners.dataflow")) {
        // enableStreamingEngine is a gcp option.
        additionalArgs.add("--enableStreamingEngine");
      }
      pipelineWrite.runWithAdditionalOptionArgs(additionalArgs).waitUntilFinish();

      assertRowCount(dataSource, firstTableName, numberOfRows);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, firstTableName);
    }
  }

  /** Generate a stream of records for testing. */
  private static class GenerateRecordsStream extends PTransform<PBegin, PCollection<TestRow>> {
    private final long numRecords;
    private final long numPerPeriod;

    public GenerateRecordsStream(long numRecords, long numPerPeriod, Duration periodLength) {
      this.numRecords = numRecords;
      this.numPerPeriod = numPerPeriod;
    }

    @Override
    public PCollection<TestRow> expand(PBegin pBegin) {
      PCollection<TestRow> pcoll =
          pBegin
              .apply(Impulse.create())
              .apply(ParDo.of(new GenerateSequenceDefinitionFn(numRecords / numPerPeriod)))
              .apply(PeriodicSequence.create())
              .apply(
                  "Add dumb key",
                  ParDo.of(
                      new DoFn<Instant, KV<Integer, Instant>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          c.output(KV.of(0, c.element()));
                        }
                      }))
              .apply(ParDo.of(new EmitSequenceFn(numRecords, numPerPeriod)))
              .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()));
      return pcoll;
    }
  }

  /** Set Periodic Sequence starting time when pipeline executation begins. */
  private static class GenerateSequenceDefinitionFn extends DoFn<byte[], SequenceDefinition> {
    private final long numPulses;

    @ProcessElement
    public void processElement(ProcessContext c) {
      Instant now = Instant.now();
      c.output(
          new SequenceDefinition(
              now, now.plus(Duration.standardSeconds(numPulses)), Duration.standardSeconds(1)));
    }

    public GenerateSequenceDefinitionFn(long numPulses) {
      this.numPulses = numPulses;
    }
  }

  private static class EmitSequenceFn extends DoFn<KV<Integer, Instant>, Long> {
    private final long numRecords;
    private final long numPerPeriod;

    public EmitSequenceFn(long numRecords, long numPerPeriod) {
      this.numRecords = numRecords;
      this.numPerPeriod = numPerPeriod;
    }

    @StateId("count")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext c, @StateId("count") ValueState<Integer> count) {
      int current = firstNonNull(count.read(), 0);
      count.write(current + 1);
      long startId = current * numPerPeriod;
      long endId = Math.min((current + 1) * numPerPeriod, numRecords);
      for (long id = startId; id < endId; ++id) {
        c.output(id);
      }
    }
  }

  /** An integration test of with write results functionality. */
  @Test
  public void testWriteWithWriteResults() throws Exception {
    String firstTableName = DatabaseTestHelper.getTestTableName("JDBCIT_WRITE");
    DatabaseTestHelper.createTable(dataSource, firstTableName);
    try {

      PCollection<KV<Integer, String>> dataCollection =
          pipelineWrite
              .apply(GenerateSequence.from(0).to(numberOfRows))
              .apply(
                  FlatMapElements.into(
                          TypeDescriptors.kvs(
                              TypeDescriptors.integers(), TypeDescriptors.strings()))
                      .via(num -> getTestDataToWrite(1)));
      PCollection<JdbcTestHelper.TestDto> resultSetCollection =
          dataCollection.apply(
              getJdbcWriteWithReturning(firstTableName)
                  .withWriteResults(
                      resultSet -> {
                        if (resultSet != null && resultSet.next()) {
                          return new JdbcTestHelper.TestDto(resultSet.getInt(1));
                        }
                        return new JdbcTestHelper.TestDto(JdbcTestHelper.TestDto.EMPTY_RESULT);
                      }));
      resultSetCollection.setCoder(JdbcTestHelper.TEST_DTO_CODER);

      PAssert.that(resultSetCollection.apply(Count.globally()))
          .containsInAnyOrder(Long.valueOf(numberOfRows));

      pipelineWrite.run().waitUntilFinish();

      assertRowCount(dataSource, firstTableName, numberOfRows);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, firstTableName);
    }
  }

  /**
   * @return {@link JdbcIO.Write} transform that writes to {@param tableName} Postgres table and
   *     returns all fields of modified rows.
   */
  private static JdbcIO.Write<KV<Integer, String>> getJdbcWriteWithReturning(String tableName) {
    return JdbcIO.<KV<Integer, String>>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
        .withStatement(String.format("insert into %s values(?, ?) returning *", tableName))
        .withPreparedStatementSetter(
            (element, statement) -> {
              statement.setInt(1, element.getKey());
              statement.setString(2, element.getValue());
            });
  }
}
