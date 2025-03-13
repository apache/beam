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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Random;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/** A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on test containers. */
@RunWith(Parameterized.class)
public class JdbcIOAutoPartitioningIT {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOAutoPartitioningIT.class);

  public static final Integer NUM_ROWS = 1_000;
  public static final String TABLE_NAME = "baseTable";

  @ClassRule public static TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<String> params() {
    return Lists.newArrayList("mysql", "postgres");
  }

  @Parameterized.Parameter(0)
  public String dbms;

  public static JdbcDatabaseContainer<?> getDb(String dbName) {
    if (dbName.equals("mysql")) {
      return mysql;
    } else {
      return postgres;
    }
  }

  // We need to implement this retrying rule because we're running ~18 pipelines that connect to
  // two databases in a few seconds. This may cause the databases to be overwhelmed, and reject
  // connections or error out. By using this rule, we ensure that each pipeline is trued twice so
  // that flakiness from databases being overwhelmed can be managed.
  @Rule
  public TestRule retryRule =
      new TestRule() {
        // We establish max number of retries at 2.
        public final int maxRetries = 2;

        @Override
        public Statement apply(Statement base, Description description) {
          return new Statement() {
            @Override
            public void evaluate() throws Throwable {
              Throwable caughtThrowable = null;
              // implement retry logic here
              for (int i = 0; i < maxRetries; i++) {
                try {
                  pipelineRead.apply(base, description);
                  base.evaluate();
                  return;
                } catch (Throwable t) {
                  caughtThrowable = t;
                  System.err.println(
                      description.getDisplayName() + ": run " + (i + 1) + " failed.");
                }
              }
              System.err.println(
                  description.getDisplayName() + ": Giving up after " + maxRetries + " failures.");
              throw Objects.requireNonNull(caughtThrowable);
            }
          };
        }
      };

  // TODO(yathu) unpin tag when the fix of
  // https://github.com/testcontainers/testcontainers-java/issues/8130
  //  released and upgraded in Beam
  @ClassRule public static JdbcDatabaseContainer<?> mysql = new MySQLContainer<>("mysql:8.2");

  @ClassRule
  public static JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>("postgres");

  @Before
  public void prepareDatabase() throws SQLException {
    pipelineRead.getOptions().setStableUniqueNames(CheckEnabled.OFF);
    DataSource dbDs = DatabaseTestHelper.getDataSourceForContainer(getDb(dbms));
    try {
      DatabaseTestHelper.createTable(
          dbDs,
          TABLE_NAME,
          Lists.newArrayList(
              KV.of("id", "INTEGER"),
              KV.of("name", "VARCHAR(50)"),
              KV.of("specialDate", "TIMESTAMP")));
    } catch (SQLException e) {
      LOG.info(
          "Exception occurred when preparing database {}. "
              + "This is expected, and the test should pass.",
          dbms,
          e);
      return;
    } catch (Exception e) {
      throw e;
    }

    final String dbmsLocal = dbms;
    pipelineWrite
        .apply(GenerateSequence.from(0).to(NUM_ROWS))
        .apply(MapElements.via(new MapRowDataFn()))
        .apply(
            JdbcIO.<RowData>write()
                .withTable(TABLE_NAME)
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal))));
    PipelineResult res = pipelineWrite.run();
    res.metrics()
        .allMetrics()
        .getDistributions()
        .forEach(
            dist -> {
              if (dist.getName().getName().contains("intsDistribution")) {
                LOG.info(
                    "Metric: {} | Min: {} | Max: {}",
                    dist.getName().getName(),
                    dist.getCommitted().getMin(),
                    dist.getCommitted().getMax());
              } else if (dist.getName().getName().contains("intsDistribution")) {
                LOG.info(
                    "Metric: {} | Min: {} | Max: {}",
                    dist.getName().getName(),
                    new DateTime(Instant.EPOCH.plus(Duration.millis(dist.getCommitted().getMin()))),
                    new DateTime(
                        Instant.EPOCH.plus(Duration.millis(dist.getCommitted().getMax()))));
              }
            });
    res.waitUntilFinish();
  }

  @DefaultSchema(JavaFieldSchema.class)
  static class RowData {
    public final Integer id;
    public final String name;
    public final DateTime specialDate;

    @SchemaCreate
    public RowData(Integer id, String name, DateTime specialDate) {
      this.id = id;
      this.name = name;
      this.specialDate = specialDate;
    }
  }

  static class MapRowDataFn extends SimpleFunction<Long, RowData> {
    private static final Distribution intDist =
        Metrics.distribution(MapRowDataFn.class, "intsDistribution");
    private static final Distribution millisDist =
        Metrics.distribution(MapRowDataFn.class, "millisDistribution");

    static String randomStr(int seed) {
      Random rnd = new Random(seed);
      StringBuilder sb = new StringBuilder(rnd.nextInt(50));
      for (int i = 0; i < sb.capacity(); i++) {
        int nextChar = rnd.nextInt();
        while (!Character.isBmpCodePoint(nextChar)) {
          nextChar = rnd.nextInt();
        }
        sb.append(Character.toChars(nextChar)[0]);
      }
      return sb.toString();
    }

    @Override
    public RowData apply(Long input) {
      Random rnd = new Random(input);
      int millisOffset = rnd.nextInt();
      millisOffset = millisOffset < 0 ? -millisOffset : millisOffset;
      int id = rnd.nextInt();
      MapRowDataFn.intDist.update(id);
      MapRowDataFn.millisDist.update(millisOffset);
      return new RowData(
          id,
          randomStr(rnd.nextInt()),
          new DateTime(Instant.EPOCH.plus(Duration.millis(millisOffset))));
    }
  }

  static class RowDataMapper implements RowMapper<RowData> {
    @Override
    public RowData mapRow(ResultSet resultSet) throws Exception {
      return new RowData(
          resultSet.getInt(1), resultSet.getString(2), new DateTime(resultSet.getTimestamp(3)));
    }
  }

  @Test
  public void testAutomaticDateTimePartitioning() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withLowerBound(new DateTime(0))
                .withUpperBound(DateTime.now())
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioning() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, Long>readWithPartitions(TypeDescriptors.longs())
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withLowerBound(Long.MIN_VALUE)
                .withUpperBound(Long.MAX_VALUE)
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  @Ignore("BEAM-13846")
  public void testAutomaticStringPartitioning() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withLowerBound("")
                .withUpperBound("999999")
                .withNumPartitions(5)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticDateTimePartitioningAutomaticRangeManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioningAutomaticRangeManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, Long>readWithPartitions(TypeDescriptors.longs())
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  @Ignore("BEAM-13846")
  public void testAutomaticStringPartitioningAutomaticRangeManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withNumPartitions(5)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioningAutomaticPartitionManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData>readWithPartitions()
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  @Ignore("BEAM-13846")
  public void testAutomaticStringPartitioningAutomaticPartitionManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticDateTimePartitioningAutomaticPartitionManagement() throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticDateTimePartitioningAutomaticPartitionManagementAndBeamRows()
      throws SQLException {
    final String dbmsLocal = dbms;
    PCollection<Row> databaseData =
        pipelineRead.apply(
            JdbcIO.<Row, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb(dbmsLocal)))
                .withTable("baseTable")
                .withRowOutput());

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }
}
