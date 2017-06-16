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

import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres, and the test dataset must exist in the
 * database. `JdbcTestDataSet` will create the read table.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/jdbc -DintegrationTestPipelineOptions='[
 *  "--postgresServerName=1.2.3.4",
 *  "--postgresUsername=postgres",
 *  "--postgresDatabaseName=myfancydb",
 *  "--postgresPassword=mypass",
 *  "--postgresSsl=false" ]'
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the jdbc pom. You'll want to activate those in addition to the normal test runner
 * invocation pipeline options.
 */
@RunWith(JUnit4.class)
public class JdbcIOIT {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOIT.class);
  private static PGSimpleDataSource dataSource;
  private static String tableName;

  @Rule
  public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule
  public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() throws SQLException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    dataSource = JdbcTestDataSet.getDataSource(options);

    tableName = JdbcTestDataSet.getWriteTableName();
    JdbcTestDataSet.createDataTable(dataSource, tableName);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    JdbcTestDataSet.cleanUpDataTable(dataSource, tableName);
  }

  /**
   * Tests writing then reading data for a postgres database.
   */
  @Test
  public void testWriteThenRead() {
    runWrite();
    runRead();
  }

  /**
   * Writes the test dataset to postgres.
   *
   * <p>This test does not attempt to validate the data - we do so in the read test. This does make
   * it harder to tell whether a test failed in the write or read phase, but the tests are much
   * easier to maintain (don't need any separate code to write test data for read tests to
   * the database.)
   */
  private void runWrite() {
    pipelineWrite.apply(GenerateSequence.from(0).to(JdbcTestDataSet.EXPECTED_ROW_COUNT))
        .apply(ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply(JdbcIO.<TestRow>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withStatement(String.format("insert into %s values(?, ?)", tableName))
            .withPreparedStatementSetter(new PrepareStatementFromTestRow()));

    pipelineWrite.run().waitUntilFinish();
  }

  /**
   * Read the test dataset from postgres and validate its contents.
   *
   * <p>When doing the validation, we wish to ensure that we both:
   * 1. Ensure *all* the rows are correct
   * 2. Provide enough information in assertions such that it is easy to spot obvious errors (e.g.
   *    all elements have a similar mistake, or "only 5 elements were generated" and the user wants
   *    to see what the problem was.
   *
   * <p>We do not wish to generate and compare all of the expected values, so this method uses
   * hashing to ensure that all expected data is present. However, hashing does not provide easy
   * debugging information (failures like "every element was empty string" are hard to see),
   * so we also:
   * 1. Generate expected values for the first and last 500 rows
   * 2. Use containsInAnyOrder to verify that their values are correct.
   * Where first/last 500 rows is determined by the fact that we know all rows have a unique id - we
   * can use the natural ordering of that key.
   */
  private void runRead() {
    PCollection<TestRow> namesAndIds =
        pipelineRead.apply(JdbcIO.<TestRow>read()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
        .withQuery(String.format("select name,id from %s;", tableName))
        .withRowMapper(new CreateTestRowOfNameAndId())
        .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(
        namesAndIds.apply("Count All", Count.<TestRow>globally()))
        .isEqualTo(JdbcTestDataSet.EXPECTED_ROW_COUNT);

    PCollection<String> consolidatedHashcode = namesAndIds
        .apply(ParDo.of(new SelectNameFn()))
        .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());
    PAssert.that(consolidatedHashcode).containsInAnyOrder(JdbcTestDataSet.EXPECTED_HASH_CODE);

    PCollection<List<TestRow>> frontOfList =
        namesAndIds.apply(Top.<TestRow>smallest(500));
    Iterable<TestRow> expectedFrontOfList = getExpectedValues(0, 500);
    PAssert.thatSingletonIterable(frontOfList).containsInAnyOrder(expectedFrontOfList);


    PCollection<List<TestRow>> backOfList =
        namesAndIds.apply(Top.<TestRow>largest(500));
    Iterable<TestRow> expectedBackOfList =
        getExpectedValues((int) (JdbcTestDataSet.EXPECTED_ROW_COUNT - 500),
            (int) JdbcTestDataSet.EXPECTED_ROW_COUNT);
    PAssert.thatSingletonIterable(backOfList).containsInAnyOrder(expectedBackOfList);

    pipelineRead.run().waitUntilFinish();
  }

  @AutoValue
  abstract static class TestRow implements Serializable, Comparable<TestRow> {
    static TestRow create(Integer id, String name) {
      return new AutoValue_JdbcIOIT_TestRow(id, name);
    }

    abstract Integer id();
    abstract String name();

    public int compareTo(TestRow other) {
      return id().compareTo(other.id());
    }
  }

  private static class CreateTestRowOfNameAndId implements JdbcIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(ResultSet resultSet) throws Exception {
      return TestRow.create(
          resultSet.getInt("id"), resultSet.getString("name"));
    }
  }

  private static class PrepareStatementFromTestRow
      implements JdbcIO.PreparedStatementSetter<TestRow> {
    @Override
    public void setParameters(TestRow element, PreparedStatement statement)
        throws SQLException {
      statement.setLong(1, element.id());
      statement.setString(2, element.name());
    }
  }

  private static TestRow expectedValueTestRow(Long seed) {
    return TestRow.create(seed.intValue(), "Testval" + seed);
  }

  private Iterable<TestRow> getExpectedValues(int rangeStart, int rangeEnd) {
    List<TestRow> ret = new ArrayList<TestRow>(rangeEnd - rangeStart + 1);
    for (int i = rangeStart; i < rangeEnd; i++) {
      ret.add(expectedValueTestRow((long) i));
    }
    return ret;
  }

  /**
   * Given a Long as a seed value, constructs a test data row used by the IT for testing writes.
   */
  private static class DeterministicallyConstructTestRowFn extends DoFn<Long, TestRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(expectedValueTestRow(c.element()));
    }
  }

  private static class SelectNameFn extends DoFn<TestRow, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().name());
    }
  }
}
