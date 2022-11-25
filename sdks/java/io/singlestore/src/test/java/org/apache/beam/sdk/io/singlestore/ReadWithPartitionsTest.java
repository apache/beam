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

import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

/** Test ReadWithPartitions. */
@RunWith(JUnit4.class)
public class ReadWithPartitionsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  public final transient Pipeline pipelineForErrorChecks = Pipeline.create();

  private static SingleStoreIO.DataSourceConfiguration dataSourceConfiguration;

  private static final int EXPECTED_ROW_COUNT = 10;

  ResultSet getMockResultSet(int from, int to) throws SQLException {
    ResultSet res = Mockito.mock(ResultSet.class, Mockito.withSettings().serializable());
    OngoingStubbing<Boolean> next = Mockito.when(res.next());
    for (int i = from; i < to; i++) {
      next = next.thenReturn(true);
    }
    next.thenReturn(false);

    OngoingStubbing<Integer> getInt = Mockito.when(res.getInt(1));
    for (int i = from; i < to; i++) {
      getInt = getInt.thenReturn(i);
    }

    OngoingStubbing<String> getString = Mockito.when(res.getString(2));
    for (int i = from; i < to; i++) {
      getString = getString.thenReturn(TestRow.getNameForSeed(i));
    }

    return res;
  }

  @Before
  public void init() throws SQLException {
    ResultSet res0 = getMockResultSet(0, EXPECTED_ROW_COUNT / 2);
    ResultSet res1 = getMockResultSet(EXPECTED_ROW_COUNT / 2, EXPECTED_ROW_COUNT);

    ResultSet resNumPartitions =
        Mockito.mock(ResultSet.class, Mockito.withSettings().serializable());
    Mockito.when(resNumPartitions.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resNumPartitions.getInt(1)).thenReturn(2);

    Statement stmtNumPartitions =
        Mockito.mock(Statement.class, Mockito.withSettings().serializable());
    Mockito.when(
            stmtNumPartitions.executeQuery(
                "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = 'db'"))
        .thenReturn(resNumPartitions);

    PreparedStatement stmt0 =
        Mockito.mock(PreparedStatement.class, Mockito.withSettings().serializable());
    Mockito.when(stmt0.executeQuery()).thenReturn(res0);

    PreparedStatement stmt1 =
        Mockito.mock(PreparedStatement.class, Mockito.withSettings().serializable());
    Mockito.when(stmt1.executeQuery()).thenReturn(res1);

    Connection conn = Mockito.mock(Connection.class, Mockito.withSettings().serializable());
    Mockito.when(conn.createStatement()).thenReturn(stmtNumPartitions);
    Mockito.when(conn.prepareStatement("SELECT * FROM (SELECT * FROM `t`) WHERE partition_id()=0"))
        .thenReturn(stmt0);
    Mockito.when(conn.prepareStatement("SELECT * FROM (SELECT * FROM `t`) WHERE partition_id()=1"))
        .thenReturn(stmt1);

    DataSource dataSource = Mockito.mock(DataSource.class, Mockito.withSettings().serializable());
    Mockito.when(dataSource.getConnection()).thenReturn(conn);

    dataSourceConfiguration =
        Mockito.mock(
            SingleStoreIO.DataSourceConfiguration.class, Mockito.withSettings().serializable());
    Mockito.when(dataSourceConfiguration.getDataSource()).thenReturn(dataSource);
    Mockito.when(dataSourceConfiguration.getDatabase()).thenReturn("db");
  }

  @Test
  public void testReadWithPartitions() {
    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>readWithPartitions()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery("SELECT * FROM `t`")
                .withRowMapper(new TestHelper.TestRowMapper()));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();
  }

  @Test
  public void testReadWithPartitionsWithTable() {
    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>readWithPartitions()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withTable("t")
                .withRowMapper(new TestHelper.TestRowMapper()));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();
  }

  @Test
  public void testReadWithPartitionsNoTableAndQuery() {
    assertThrows(
        "One of withTable() or withQuery() is required",
        IllegalArgumentException.class,
        () ->
            pipelineForErrorChecks.apply(
                SingleStoreIO.<TestRow>readWithPartitions()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withRowMapper(new TestHelper.TestRowMapper())));
  }

  @Test
  public void testReadWithPartitionsBothTableAndQuery() {
    assertThrows(
        "withTable() can not be used together with withQuery()",
        IllegalArgumentException.class,
        () ->
            pipelineForErrorChecks.apply(
                SingleStoreIO.<TestRow>readWithPartitions()
                    .withDataSourceConfiguration(dataSourceConfiguration)
                    .withTable("t")
                    .withQuery("SELECT * FROM `t`")
                    .withRowMapper(new TestHelper.TestRowMapper())));
  }
}
