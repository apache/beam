package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

/**
 * Manipulates test data used by the JdbcIO tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after data store creation rather
 * than every time (which can be more fragile.)
 */
public class JdbcTestDataSet {

  /**
   * Use this to create the read tables before IT read tests.
   *
   * <p>To invoke this class, you can use this command line:
   * mvn test-compile exec:java -Dexec.mainClass=org.apache.beam.sdk.io.jdbc.JdbcTestDataSet \
   *   -Dexec.args="--postgresIp=1.1.1.1 --postgresUsername=postgres --postgresDatabaseName=myfancydb \
   *   --postgresPassword=yourpassword --postgresSsl=false" \
   *   -Dexec.classpathScope=test
   * @param args Please pass options from PostgresTestOptions used for connection to postgres as shown above.
   */
  public static void main(String[] args) throws SQLException {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    PipelineOptionsFactory.register(PostgresTestOptions.class);
    PostgresTestOptions options = PipelineOptionsFactory.fromArgs(args).create().as(PostgresTestOptions.class);

    dataSource.setDatabaseName(options.getPostgresDatabaseName());
    dataSource.setServerName(options.getPostgresIp());
    dataSource.setPortNumber(options.getPostgresPort());
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(options.getPostgresSsl());

    createReadDataTable(dataSource);
  }

  public final static String READ_TABLE_NAME = "BEAMTESTREAD";

  public static void createReadDataTable(DataSource dataSource) throws SQLException {
    createDataTable_(dataSource, READ_TABLE_NAME);
  }

  public static String createWriteDataTable(DataSource dataSource) throws SQLException {
    String tableName = "BEAMTEST".concat(new Long(new Date().getTime()).toString());
    createDataTable_(dataSource, tableName);
    return tableName;
  }

  public static void createDataTable_(DataSource dataSource, String tableName) throws SQLException {
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

    System.out.println("Created table " + tableName);
  }

  public static void cleanUpDataTable(DataSource dataSource, String tableName) throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.executeUpdate(String.format("drop table %s", tableName));
        }
      }
    }
  }

}
