package org.apache.beam.sdk.io.jdbc;

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

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.Assert.assertEquals;


@RunWith(JUnit4.class)
public class JdbcIOIT {

  public PGSimpleDataSource connectToDatabase() throws SQLException {
    // test options from the maven config are made available in TestPipeline.testingPipelineOptions()- see
    // https://github.com/apache/incubator-beam/blob/master/examples/java/pom.xml#L167
    // for an example of adding settings in maven config
    // beamTestPipelineOptions come through in TestPipeline.testingPipelineOptions()

    PipelineOptionsFactory.register(JdbcTestOptions.class);
    JdbcTestOptions options = TestPipeline.testingPipelineOptions()
        .as(JdbcTestOptions.class);
    PGSimpleDataSource dataSource = new PGSimpleDataSource();

    // todo - we might be able to load all of these from one string?
    dataSource.setDatabaseName("target/beam"); // TODO - load from prop (most of below need this too)
    dataSource.setServerName(options.getPostgresIp()); // all options will be loaded like this from JdbcTestOptions
    dataSource.setPortNumber(12345); // todo
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(true);

    return dataSource;
  }

  /*
  public void createDataTable(DataSource dataSource) throws SQLException {
    // something like this will need to happen in tests on a new server, but likely it will happen in perfkit, not here
    // alternatively, we may have a pipelineoption indicating whether we want to re-use the database or create a new one
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate("create table BEAMTEST(id INT, name VARCHAR(500))");
    }

    // We would want to move this into shared code with JdbcIOTest if it doesn't happen in perfkit code
    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    connection.setAutoCommit(false);
    try (PreparedStatement preparedStatement =
             connection.prepareStatement("insert into BEAM " + "values (?,?)")) {
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
  */

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    DataSource dataSource = connectToDatabase();
    // createDataTable(dataSource); // todo - we would like this to happen prior to the test in the future, only if a new server is created.
    TestPipeline pipeline = TestPipeline.create();

    // the below test is copied from JdbcIOTest
    PCollection<KV<String, Integer>> output = pipeline.apply(
        JdbcIO.<KV<String, Integer>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withQuery("select name,id from BEAMTEST")
            .withRowMapper(new JdbcIO.RowMapper<KV<String, Integer>>() {
              @Override
              public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
                  KV<String, Integer> kv =
                      KV.of(resultSet.getString("name"), resultSet.getInt("id"));
                  return kv;
              }
            })
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PAssert.thatSingleton(
        output.apply("Count All", Count.<KV<String, Integer>>globally()))
        .isEqualTo(1000L);

    PAssert.that(output
        .apply("Count Scientist", Count.<String, Integer>perKey())
    ).satisfies(new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Long>> input) {
        for (KV<String, Long> element : input) {
          assertEquals(element.getKey(), 100L, element.getValue().longValue());
        }
        return null;
      }
    });

    pipeline.run();

    // TODO wait for it to finish
    // TODO clean up database table if needed
  }
}
