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

import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
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

/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>This test requires a running instance of Postgres. Pass in connection information using
 * PipelineOptions:
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
 * <p>Please see 'build_rules.gradle' file for instructions regarding
 * running this test using Beam performance testing framework.</p>
 */
@RunWith(JUnit4.class)
public class JdbcIOIT {

  private static int numberOfRows;
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

    numberOfRows = options.getNumberOfRecords();
    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    tableName = DatabaseTestHelper.getTestTableName("IT");
    DatabaseTestHelper.createTable(dataSource, tableName);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
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
   * <p>This method does not attempt to validate the data - we do so in the read test. This does
   * make it harder to tell whether a test failed in the write or read phase, but the tests are much
   * easier to maintain (don't need any separate code to write test data for read tests to
   * the database.)
   */
  private void runWrite() {
    pipelineWrite.apply(GenerateSequence.from(0).to(numberOfRows))
        .apply(ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply(JdbcIO.<TestRow>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withStatement(String.format("insert into %s values(?, ?)", tableName))
            .withPreparedStatementSetter(new JdbcTestHelper.PrepareStatementFromTestRow()));

    pipelineWrite.run().waitUntilFinish();
  }

  /**
   * Read the test dataset from postgres and validate its contents.
   *
   * <p>When doing the validation, we wish to ensure that we:
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
        .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
        .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(namesAndIds.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRows);

    PCollection<String> consolidatedHashcode = namesAndIds
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

    pipelineRead.run().waitUntilFinish();
  }
}
