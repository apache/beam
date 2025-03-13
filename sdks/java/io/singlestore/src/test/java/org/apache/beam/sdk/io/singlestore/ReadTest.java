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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

/** Test Read. */
@RunWith(JUnit4.class)
public class ReadTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  public final transient Pipeline pipelineForErrorChecks = Pipeline.create();

  private static SingleStoreIO.DataSourceConfiguration dataSourceConfiguration;
  private static PreparedStatement stmt;
  private static Connection conn;

  private static int setIntCalls = 0;

  private static class SetIntCall implements Serializable, Answer<Void> {
    @Override
    public Void answer(InvocationOnMock invocation) {
      setIntCalls++;
      return null;
    }
  }

  private static final int EXPECTED_ROW_COUNT = 10;

  private static class TestStatmentPreparator implements SingleStoreIO.StatementPreparator {
    @Override
    public void setParameters(PreparedStatement preparedStatement) throws Exception {
      preparedStatement.setInt(1, 10);
    }
  }

  @Before
  public void init() throws SQLException {
    ResultSet res = Mockito.mock(ResultSet.class, Mockito.withSettings().serializable());
    OngoingStubbing<Boolean> next = Mockito.when(res.next());
    for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
      next = next.thenReturn(true);
    }
    next.thenReturn(false);

    OngoingStubbing<Integer> getInt = Mockito.when(res.getInt(1));
    for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
      getInt = getInt.thenReturn(i);
    }

    OngoingStubbing<String> getString = Mockito.when(res.getString(2));
    for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
      getString = getString.thenReturn(TestRow.getNameForSeed(i));
    }

    stmt = Mockito.mock(PreparedStatement.class, Mockito.withSettings().serializable());
    Mockito.when(stmt.executeQuery()).thenReturn(res);

    conn = Mockito.mock(Connection.class, Mockito.withSettings().serializable());
    Mockito.when(conn.prepareStatement("SELECT * FROM `t`")).thenReturn(stmt);

    DataSource dataSource = Mockito.mock(DataSource.class, Mockito.withSettings().serializable());
    Mockito.when(dataSource.getConnection()).thenReturn(conn);

    dataSourceConfiguration =
        Mockito.mock(
            SingleStoreIO.DataSourceConfiguration.class, Mockito.withSettings().serializable());
    Mockito.when(dataSourceConfiguration.getDataSource()).thenReturn(dataSource);
  }

  @Test
  public void testRead() {
    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>read()
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
  public void testReadWithStatementPreparator() throws SQLException {
    Mockito.when(conn.prepareStatement("SELECT * FROM `t` WHERE id < ?")).thenReturn(stmt);
    Mockito.doAnswer(new SetIntCall()).when(stmt).setInt(1, 10);

    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery("SELECT * FROM `t` WHERE id < ?")
                .withRowMapper(new TestHelper.TestRowMapper())
                .withStatementPreparator(new TestStatmentPreparator()));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();

    assertEquals(1, setIntCalls);
  }

  @Test
  public void testReadWithTable() {
    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>read()
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
  public void testReadWithoutOutputParallelization() {
    PCollection<TestRow> rows =
        pipeline.apply(
            SingleStoreIO.<TestRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery("SELECT * FROM `t`")
                .withRowMapper(new TestHelper.TestRowMapper())
                .withOutputParallelization(false));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();
  }

  @Test
  public void testReadNoTableAndQuery() {
    assertThrows(
        "One of withTable() or withQuery() is required",
        IllegalArgumentException.class,
        () -> {
          pipelineForErrorChecks.apply(
              SingleStoreIO.<TestRow>read()
                  .withDataSourceConfiguration(dataSourceConfiguration)
                  .withRowMapper(new TestHelper.TestRowMapper())
                  .withOutputParallelization(false));
        });
  }

  @Test
  public void testReadBothTableAndQuery() {
    assertThrows(
        "withTable() can not be used together with withQuery()",
        IllegalArgumentException.class,
        () -> {
          pipelineForErrorChecks.apply(
              SingleStoreIO.<TestRow>read()
                  .withDataSourceConfiguration(dataSourceConfiguration)
                  .withTable("t")
                  .withQuery("SELECT * FROM `t`")
                  .withRowMapper(new TestHelper.TestRowMapper())
                  .withOutputParallelization(false));
        });
  }
}
