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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;


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
  private static PGSimpleDataSource dataSource;
  private static String writeTableName;

  @BeforeClass
  public static void setup() throws SQLException {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    // We do dataSource set up in BeforeClass rather than Before since we don't need to create a new
    // dataSource for each test.
    dataSource = JdbcTestDataSet.getDataSource(options);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    // Only do write table clean up once for the class since we don't want to clean up after both
    // read and write tests, only want to do it once after all the tests are done.
    JdbcTestDataSet.cleanUpDataTable(dataSource, writeTableName);
  }

  private static class CreateKVOfNameAndId implements JdbcIO.RowMapper<KV<String, Integer>> {
    @Override
    public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
      KV<String, Integer> kv =
          KV.of(resultSet.getString("name"), resultSet.getInt("id"));
      return kv;
    }
  }

  private static class PutKeyInColumnOnePutValueInColumnTwo
      implements JdbcIO.PreparedStatementSetter<KV<Integer, String>> {
    @Override
    public void setParameters(KV<Integer, String> element, PreparedStatement statement)
                    throws SQLException {
      statement.setInt(1, element.getKey());
      statement.setString(2, element.getValue());
    }
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  /**
   * Does a test read of a few rows from a postgres database.
   *
   * <p>Note that IT read tests must not do any data table manipulation (setup/clean up.)
   * @throws SQLException
   */
  @Test
  public void testRead() throws SQLException {
    String writeTableName = JdbcTestDataSet.READ_TABLE_NAME;

    PCollection<KV<String, Integer>> output = pipeline.apply(JdbcIO.<KV<String, Integer>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withQuery("select name,id from " + writeTableName)
            .withRowMapper(new CreateKVOfNameAndId())
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    // TODO: validate actual contents of rows, not just count.
    PAssert.thatSingleton(
        output.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo(1000L);

    List<KV<String, Long>> expectedCounts = new ArrayList<>();
    for (String scientist : JdbcTestDataSet.SCIENTISTS) {
      expectedCounts.add(KV.of(scientist, 100L));
    }
    PAssert.that(output.apply("Count Scientist", Count.<String, Integer>perKey()))
        .containsInAnyOrder(expectedCounts);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Tests writes to a postgres database.
   *
   * <p>Write Tests must clean up their data - in this case, it uses a new table every test run so
   * that it won't interfere with read tests/other write tests. It uses finally to attempt to
   * clean up data at the end of the test run.
   * @throws SQLException
   */
  @Test
  public void testWrite() throws SQLException {
    writeTableName = JdbcTestDataSet.createWriteDataTable(dataSource);

    ArrayList<KV<Integer, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<Integer, String> kv = KV.of(i, "Test");
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(JdbcIO.<KV<Integer, String>>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withStatement(String.format("insert into %s values(?, ?)", writeTableName))
            .withPreparedStatementSetter(new PutKeyInColumnOnePutValueInColumnTwo()));

    pipeline.run().waitUntilFinish();

    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("select count(*) from " + writeTableName)) {
      resultSet.next();
      int count = resultSet.getInt(1);
      Assert.assertEquals(2000, count);
    }
    // TODO: Actually verify contents of the rows.
  }
}
