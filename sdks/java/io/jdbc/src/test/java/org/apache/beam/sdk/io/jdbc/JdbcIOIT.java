package org.apache.beam.sdk.io.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import javax.sql.DataSource;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;


/**
 * Runs tests to validate JdbcIO works with a real instance of Postgres. You'll need
 * a running instance of postgres to validate this test and you'll need to make sure the test data
 * set exists in the database - if not, you can run JdbcTestDataSet and it will create the read
 * table for you.
 *
 * <p>You can run just this test by doing the following:
 * mvn test-compile compile failsafe:integration-test -D beamTestPipelineOptions='[
 * "--postgresIp=1.2.3.4",
 * "--postgresUsername=postgres",
 * "--postgresDatabaseName=myfancydb",
 * "--postgresPassword=yourpassword",
 * "--postgresSsl=false"
 * ]' -DskipITs=false -Dit.test=org.apache.beam.sdk.io.jdbc.JdbcIOIT -DfailIfNoTests=false
 */
@RunWith(JUnit4.class)
public class JdbcIOIT {

  public PGSimpleDataSource getDataSource() throws SQLException {
    PipelineOptionsFactory.register(PostgresTestOptions.class);
    PostgresTestOptions options = TestPipeline.testingPipelineOptions()
        .as(PostgresTestOptions.class);
    PGSimpleDataSource dataSource = new PGSimpleDataSource();

    // Tests must receive parameters for connections from PipelineOptions
    // Parameters should be generic to all tests that use a particular datasource, not
    // the particular test.
    dataSource.setDatabaseName(options.getPostgresDatabaseName());
    dataSource.setServerName(options.getPostgresIp());
    dataSource.setPortNumber(options.getPostgresPort());
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(options.getPostgresSsl());

    return dataSource;
  }

  // These static classes are used since if they were inlined using anonymous classes, they would cause the JdbcIOIT
  // class as a whole to be serialized.

  private static class TestRowMapper implements JdbcIO.RowMapper<KV<String, Integer>> {
    @Override
    public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
      KV<String, Integer> kv =
          KV.of(resultSet.getString("name"), resultSet.getInt("id"));
      return kv;
    }
  }

  private static class ValidateCountFn implements SerializableFunction<Iterable<KV<String, Long>>, Void> {
    @Override
    public Void apply(Iterable<KV<String, Long>> input) {
      for (KV<String, Long> element : input) {
        assertEquals(element.getKey(), 100L, element.getValue().longValue());
      }
      return null;
    }
  };

  private static class TestStatementSetter implements JdbcIO.PreparedStatementSetter<KV<Integer, String>> {
    @Override
    public void setParameters(KV<Integer, String> element, PreparedStatement statement)
                    throws SQLException {
      statement.setInt(1, element.getKey());
      statement.setString(2, element.getValue());
    }
  }

  /**
   * Does a test read of a few rows from a postgres database.
   *
   * <p>Note that IT read tests must not do any data table manipulation (setup/clean up.)
   * @throws SQLException
   */
  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws SQLException {
    DataSource dataSource = getDataSource();

    String tableName = JdbcTestDataSet.READ_TABLE_NAME;

    TestPipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> output = pipeline.apply(JdbcIO.<KV<String, Integer>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withQuery("select name,id from " + tableName)
            .withRowMapper(new TestRowMapper())
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    // TODO: validate actual contents of rows, not just count.
    PAssert.thatSingleton(
        output.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo(1000L);

    PAssert.that(output
        .apply("Count Scientist", Count.<String, Integer>perKey())
    ).satisfies(new ValidateCountFn());

    pipeline.run().waitUntilFinish();
  }


  /**
   * Tests writes to a real live postgres database.
   *
   * Write Tests must clean up their data - in this case, it uses a new table every test run so
   * that it won't interfere with read tests/other write tests. It uses finally to attempt to clean up data at the end
   * of the test run.
   * @throws SQLException
   */
  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws SQLException {
    DataSource dataSource = getDataSource();

    String tableName = null;

    try {
      tableName = JdbcTestDataSet.createWriteDataTable(dataSource);

      TestPipeline pipeline = TestPipeline.create();

      ArrayList<KV<Integer, String>> data = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        KV<Integer, String> kv = KV.of(i, "Test");
        data.add(kv);
      }
      pipeline.apply(Create.of(data))
          .apply(JdbcIO.<KV<Integer, String>>write()
              .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
              .withStatement(String.format("insert into %s values(?, ?)", tableName))
              .withPreparedStatementSetter(new TestStatementSetter()));

      pipeline.run().waitUntilFinish();

      PAssert.that(output
          .apply("Count Scientist", Count.<String, Integer>perKey())
      ).satisfies(new ValidateCountFn());
      pipeline.run().waitUntilFinish();

    } finally {
      // cleanup!
      if (tableName != null) {
        try (Connection connection = dataSource.getConnection()) {
          try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format("drop table if exists %s", tableName));
          }
        }
      }
      // TODO: Actually verify contents of the rows.
    } finally {
      JdbcTestDataSet.cleanUpDataTable(dataSource, tableName);
    }
  }

}
