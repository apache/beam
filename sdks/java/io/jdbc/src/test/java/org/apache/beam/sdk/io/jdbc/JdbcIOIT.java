package org.apache.beam.sdk.io.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;


/**
 * Runs tests to validate JdbcIO works with a real instance of a datastore. You'll need
 * a running instance of postgres to validate this test.
 *
 * <p>Cleanup notes: this test cleans up after itself, but only if it gets a chance. It
 * is intended to be run in an db that is cleaned up regularly (ie, server reset every
 * week.)
 *
 * <p>You can run just this test by doing the following:
 * mvn test-compile compile failsafe:integration-test -D beamTestPipelineOptions='[
 * "--postgresIp=1.2.3.4",
 * "--postgresUsername=postgres",
 * "--postgresDatabaseName=myfancydb",
 * "--postgresPassword=yourpassword",
 * "--postgresSsl=false"
 * ]'
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

  public String createDataTable(DataSource dataSource) throws SQLException {
    String tableName = "BEAMTEST".concat(new Long(new Date().getTime()).toString());
    try (Connection connection = dataSource.getConnection()) {
      // something like this will need to happen in tests on a newly created postgres server,
      // but likely it will happen in perfkit, not here
      // alternatively, we may have a pipelineoption indicating whether we want to
      // re-use the database or create a new one
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }


      // We would want to move this into shared code with JdbcIOTest if
      // it doesn't happen in perfkit code
      String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
          "Newton", "Bohr", "Galilei", "Maxwell"};
      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement =
               connection.prepareStatement(
                   String.format("insert into %s values (?,?)", tableName))) {
          for (int i = 0; i < 1000; i++) {
            int index = i % scientists.length;
            preparedStatement.clearParameters();
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, scientists[index]);
            preparedStatement.executeUpdate();
          }
        }
        connection.commit();
      }
      return tableName;
    }

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

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    // TODO - this method still does some stuff that doesn't belong in the IT
    DataSource dataSource = getDataSource();

    String tableName = null;

    try {
      tableName = createDataTable(dataSource);

      TestPipeline pipeline = TestPipeline.create();

      PCollection<KV<String, Integer>> output = pipeline.apply(JdbcIO.<KV<String, Integer>>read()
              .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
              .withQuery("select name,id from " + tableName)
              .withRowMapper(new TestRowMapper())
              .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

      PAssert.thatSingleton(
          output.apply("Count All", Count.<KV<String, Integer>>globally()))
          .isEqualTo(1000L);

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
    }
  }
}
