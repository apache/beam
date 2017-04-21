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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the JdbcIO.
 */
public class JdbcIOTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOTest.class);

  private static NetworkServerControl derbyServer;
  private static ClientDataSource dataSource;

  private static int port;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startDatabase() throws Exception {
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    LOG.info("Starting Derby database on {}", port);

    System.setProperty("derby.stream.error.file", "target/derby.log");

    derbyServer = new NetworkServerControl(InetAddress.getByName("localhost"), port);
    StringWriter out = new StringWriter();
    derbyServer.start(new PrintWriter(out));
    boolean started = false;
    int count = 0;
    // Use two different methods to detect when server is started:
    // 1) Check the server stdout for the "started" string
    // 2) wait up to 15 seconds for the derby server to start based on a ping
    // on faster machines and networks, this may return very quick, but on slower
    // networks where the DNS lookups are slow, this may take a little time
    while (!started && count < 30) {
      if (out.toString().contains("started")) {
        started = true;
      } else {
        count++;
        Thread.sleep(500);
        try {
          derbyServer.ping();
          started = true;
        } catch (Throwable t) {
          //ignore, still trying to start
        }
      }
    }

    dataSource = new ClientDataSource();
    dataSource.setCreateDatabase("create");
    dataSource.setDatabaseName("target/beam");
    dataSource.setServerName("localhost");
    dataSource.setPortNumber(port);


    JdbcTestDataSet.createReadDataTable(dataSource);
  }

  @AfterClass
  public static void shutDownDatabase() throws Exception {
    try {
      JdbcTestDataSet.cleanUpDataTable(dataSource, JdbcTestDataSet.READ_TABLE_NAME);
    } finally {
      if (derbyServer != null) {
        derbyServer.shutdown();
      }
    }
  }

  @Test
  public void testDataSourceConfigurationDataSource() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(dataSource);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationDriverAndUrl() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(
        "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://localhost:" + port + "/target/beam");
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationUsernameAndPassword() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(
        "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://localhost:" + port + "/target/beam")
        .withUsername("sa")
        .withPassword("sa");
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullPassword() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(
        "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://localhost:" + port + "/target/beam")
        .withUsername("sa")
        .withPassword(null);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullUsernameAndPassword() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(
        "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://localhost:" + port + "/target/beam")
        .withUsername(null)
        .withPassword(null);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {

    PCollection<KV<String, Integer>> output = pipeline.apply(
        JdbcIO.<KV<String, Integer>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
            .withQuery("select name,id from " + JdbcTestDataSet.READ_TABLE_NAME)
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
  }

   @Test
   @Category(NeedsRunner.class)
   public void testReadWithSingleStringParameter() throws Exception {

     PCollection<KV<String, Integer>> output = pipeline.apply(
             JdbcIO.<KV<String, Integer>>read()
                     .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                     .withQuery(String.format("select name,id from %s where name = ?",
                         JdbcTestDataSet.READ_TABLE_NAME))
                     .withStatementPreparator(new JdbcIO.StatementPreparator() {
                       @Override
                       public void setParameters(PreparedStatement preparedStatement)
                               throws Exception {
                         preparedStatement.setString(1, "Darwin");
                       }
                     })
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
             output.apply("Count One Scientist", Count.<KV<String, Integer>>globally()))
             .isEqualTo(100L);

     pipeline.run();
   }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {

    String tableName = JdbcTestDataSet.createWriteDataTable(dataSource);
    try {
      ArrayList<KV<Integer, String>> data = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        KV<Integer, String> kv = KV.of(i, "Test");
        data.add(kv);
      }
      pipeline.apply(Create.of(data))
          .apply(JdbcIO.<KV<Integer, String>>write()
              .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                  "org.apache.derby.jdbc.ClientDriver",
                  "jdbc:derby://localhost:" + port + "/target/beam"))
              .withStatement(String.format("insert into %s values(?, ?)", tableName))
              .withPreparedStatementSetter(
                  new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
                public void setParameters(
                    KV<Integer, String> element, PreparedStatement statement) throws Exception {
                  statement.setInt(1, element.getKey());
                  statement.setString(2, element.getValue());
                }
              }));

      pipeline.run();

      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          try (ResultSet resultSet = statement.executeQuery("select count(*) from "
                + tableName)) {
            resultSet.next();
            int count = resultSet.getInt(1);

            Assert.assertEquals(2000, count);
          }
        }
      }
    } finally {
      JdbcTestDataSet.cleanUpDataTable(dataSource, tableName);
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithEmptyPCollection() throws Exception {

    pipeline
        .apply(Create.empty(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of())))
        .apply(JdbcIO.<KV<Integer, String>>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam"))
            .withStatement("insert into BEAM values(?, ?)")
            .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
              public void setParameters(KV<Integer, String> element, PreparedStatement statement)
                  throws Exception {
                statement.setInt(1, element.getKey());
                statement.setString(2, element.getValue());
              }
            }));

    pipeline.run();
  }
}
