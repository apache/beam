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
import java.util.Random;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

/** A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on test containers. */
@RunWith(JUnit4.class)
public class JdbcIOAutoPartitioningIT {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOAutoPartitioningIT.class);

  public static final Integer NUM_ROWS = 1_000;
  public static final String TABLE_NAME = "baseTable";

  @ClassRule public static TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  public static JdbcDatabaseContainer<?> getDb() {
    return db;
  }

  @ClassRule public static JdbcDatabaseContainer<?> db = new MySQLContainer<>("mysql");

  @BeforeClass
  public static void prepareDatabase() throws SQLException {
    DataSource mysqlDs = DatabaseTestHelper.getDataSourceForContainer(getDb());
    DatabaseTestHelper.createTable(
        mysqlDs,
        TABLE_NAME,
        Lists.newArrayList(
            KV.of("id", "INTEGER"),
            KV.of("name", "VARCHAR(50)"),
            KV.of("specialDate", "TIMESTAMP")));

    pipelineWrite
        .apply(GenerateSequence.from(0).to(NUM_ROWS))
        .apply(MapElements.via(new MapRowDataFn()))
        .apply(
            JdbcIO.<RowData>write()
                .withTable(TABLE_NAME)
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb())));
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
          String.valueOf(rnd.nextDouble()),
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
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
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
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, Long>readWithPartitions(TypeDescriptors.longs())
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withLowerBound(Long.MIN_VALUE)
                .withUpperBound(Long.MAX_VALUE)
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticStringPartitioning() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
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
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioningAutomaticRangeManagement() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, Long>readWithPartitions(TypeDescriptors.longs())
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticStringPartitioningAutomaticRangeManagement() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withNumPartitions(5)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioningAutomaticPartitionManagement() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData>readWithPartitions()
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticStringPartitioningAutomaticPartitionManagement() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticDateTimePartitioningAutomaticPartitionManagement() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(getDb()))
                .withTable("baseTable")
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }
}
