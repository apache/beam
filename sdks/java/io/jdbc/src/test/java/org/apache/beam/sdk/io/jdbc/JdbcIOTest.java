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

import java.io.Serializable;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the JdbcIO.
 */
public class JdbcIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIOTest.class);

  private ClientDataSource dataSource;

  @Before
  public void setup() throws Exception {
    System.setProperty("derby.stream.error.file", "target/derby.log");

    NetworkServerControl derbyServer = new NetworkServerControl(InetAddress.getByName
        ("localhost"), 1527);
    derbyServer.start(null);

    dataSource = new ClientDataSource();
    dataSource.setCreateDatabase("create");
    dataSource.setDatabaseName("target/beam");
    dataSource.setServerName("localhost");
    dataSource.setPortNumber(1527);

    Connection connection = dataSource.getConnection();

    Statement statement = connection.createStatement();
    try {
      statement.executeUpdate("create table BEAM(id INT, name VARCHAR(500))");
      statement.executeUpdate("create table TEST(ID INT, NAME VARCHAR(200))");
    } catch (Exception e) {
      LOGGER.warn("Can't create table BEAM, it probably already exists", e);
    } finally {
      statement.close();
    }

    statement = connection.createStatement();
    statement.executeUpdate("delete from BEAM");
    statement.close();

    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    for (int i = 1; i <= 1000; i++) {
      int index = i % scientists.length;
      PreparedStatement preparedStatement = connection.prepareStatement("insert into BEAM "
          + "values (?,?)");
      preparedStatement.setInt(1, i);
      preparedStatement.setString(2, scientists[index]);
      preparedStatement.executeUpdate();
      preparedStatement.close();
    }

    connection.commit();
    connection.close();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<JdbcDataRecord> output = pipeline.apply(
        JdbcIO.read()
            .withDataSource(dataSource)
            .withQuery("select * from BEAM"));

    PAssert.thatSingleton(
        output.apply("Count All", Count.<JdbcDataRecord>globally()))
        .isEqualTo(1000L);

    PAssert.that(output
        .apply("Map Scientist", MapElements.via(
            new SimpleFunction<JdbcDataRecord, KV<String, Void>>() {
              public KV<String, Void> apply(JdbcDataRecord input) {
                // find NAME column id
                int index = -1;
                for (int i = 0; i < input.getColumnNames().length; i++) {
                  if (input.getColumnNames()[i].equals("NAME")) {
                    index = i;
                    break;
                  }
                }
                if (index != -1) {
                  return KV.of(input.getColumnValues()[index].toString(), null);
                } else {
                  return KV.of(null, null);
                }
              }
            }))
        .apply("Count Scientist", Count.<String, Void>perKey())
    ).satisfies(new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Long>> input) {
        for (KV<String, Long> element : input) {
          assertEquals(100L, element.getValue().longValue());
        }
        return null;
      }
    });

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    ArrayList<JdbcDataRecord> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      JdbcDataRecord record = new JdbcDataRecord(2);
      record.getTableNames()[0] = "TEST";
      record.getColumnNames()[0] = "ID";
      record.getColumnTypes()[0] = Types.INTEGER;
      record.getColumnValues()[0] = i;
      record.getTableNames()[1] = "TEST";
      record.getColumnNames()[1] = "NAME";
      record.getColumnTypes()[1] = Types.VARCHAR;
      record.getColumnValues()[1] = "Test";
      data.add(record);
    }
    pipeline.apply(Create.of(data))
        .apply(JdbcIO.write().withDataSource(dataSource));

    pipeline.run();

    Connection connection = dataSource.getConnection();

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select * from TEST");
    int count = 0;
    while (resultSet.next()) {
      count++;
    }

    Assert.assertEquals(1000, count);
  }

  @After
  public void cleanup() throws Exception {
    try {
      Connection connection = dataSource.getConnection();

      Statement statement = connection.createStatement();
      statement.executeUpdate("drop table BEAM");
      statement.executeUpdate("drop table TEST");
      statement.close();

      connection.close();
    } catch (Exception e) {
      // nothing to do
    }
  }

}
