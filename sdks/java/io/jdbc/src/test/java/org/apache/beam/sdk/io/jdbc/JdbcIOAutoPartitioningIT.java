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
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
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
import org.testcontainers.containers.MySQLContainer;

/** A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on test containers. */
@RunWith(JUnit4.class)
public class JdbcIOAutoPartitioningIT {

  public static final Integer NUM_ROWS = 1_000;
  public static final String TABLE_NAME = "baseTable";

  @ClassRule public static TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @ClassRule public static MySQLContainer<?> mysql = new MySQLContainer<>("mysql");

  // @ClassRule
  // public static PostgreSQLContainer<?> postgres =
  //     new PostgreSQLContainer<>("postgres");

  @BeforeClass
  public static void prepareDatabase() throws SQLException {
    DataSource mysqlDs = DatabaseTestHelper.getDataSourceForContainer(mysql);
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
                    voide -> DatabaseTestHelper.getDataSourceForContainer(mysql)));
    pipelineWrite.run().waitUntilFinish();
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
    @Override
    public RowData apply(Long input) {
      Random rnd = new Random(input);
      int millisOffset = rnd.nextInt();
      millisOffset = millisOffset < 0 ? -millisOffset : millisOffset;
      return new RowData(
          rnd.nextInt(),
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
  public void testAutomaticDateTimePartitioningMySQL() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, DateTime>readWithPartitions(TypeDescriptor.of(DateTime.class))
                .withPartitionColumn("specialDate")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(mysql))
                .withTable("baseTable")
                // TODO(pabloem): do min/max inference
                .withLowerBound(new DateTime(0))
                .withUpperBound(DateTime.now())
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticLongPartitioningMySQL() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, Long>readWithPartitions(TypeDescriptors.longs())
                .withPartitionColumn("id")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(mysql))
                .withTable("baseTable")
                // TODO(pabloem): do min/max inference
                .withLowerBound(Long.MIN_VALUE)
                .withUpperBound(Long.MAX_VALUE)
                .withNumPartitions(10)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }

  @Test
  public void testAutomaticStringPartitioningMySQL() throws SQLException {
    PCollection<RowData> databaseData =
        pipelineRead.apply(
            JdbcIO.<RowData, String>readWithPartitions(TypeDescriptors.strings())
                .withPartitionColumn("name")
                .withDataSourceProviderFn(
                    voide -> DatabaseTestHelper.getDataSourceForContainer(mysql))
                .withTable("baseTable")
                // TODO(pabloem): do min/max inference
                .withLowerBound("00000")
                .withUpperBound("999999")
                .withNumPartitions(5)
                .withRowMapper(new RowDataMapper()));

    PAssert.that(databaseData.apply(Count.globally())).containsInAnyOrder(NUM_ROWS.longValue());
    pipelineRead.run().waitUntilFinish();
  }
}
