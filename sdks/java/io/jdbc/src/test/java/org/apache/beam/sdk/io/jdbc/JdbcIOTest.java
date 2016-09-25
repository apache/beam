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
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import org.apache.beam.sdk.coders.SerializableCoder;
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

  /**
   * Example of {@link PCollection} element that a
   * {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper} can return.
   */
  public class JdbcDataRecord implements Serializable {

    private String[] tableNames;
    private String[] columnNames;
    private Object[] columnValues;
    private int[] columnTypes;

    public JdbcDataRecord() {
    }

    public JdbcDataRecord(int size) {
      this.tableNames = new String[size];
      this.columnNames = new String[size];
      this.columnValues = new Object[size];
      this.columnTypes = new int[size];
    }

    public String[] getTableNames() {
      return tableNames;
    }

    public void setTableNames(String[] tableName) {
      this.tableNames = tableName;
    }

    public String[] getColumnNames() {
      return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
      this.columnNames = columnNames;
    }

    public Object[] getColumnValues() {
      return columnValues;
    }

    public void setColumnValues(Object[] columnValues) {
      this.columnValues = columnValues;
    }

    public int[] getColumnTypes() {
      return columnTypes;
    }

    public void setColumnTypes(int[] columnTypes) {
      this.columnTypes = columnTypes;
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < tableNames.length; i++) {
        builder.append("Table: ").append(tableNames[i]).append(" | Column: ")
            .append(columnNames[i]).append(" | Type: ").append(columnTypes[i])
            .append(" | Value: ").append(columnValues[i]).append("\n");
      }
      return builder.toString();
    }

  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<JdbcDataRecord> output = pipeline.apply(
        JdbcIO.read()
            .withDataSource(dataSource)
            .withQuery("select * from BEAM")
            .withRowMapper(new JdbcIO.RowMapper<JdbcDataRecord>() {
              @Override
              public JdbcDataRecord mapRow(ResultSet resultSet) {
                JdbcDataRecord record = null;
                try {
                  ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                  record = new JdbcDataRecord(resultSetMetaData.getColumnCount());
                  for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    record.getColumnNames()[i - 1] = columnName;
                    String tableName = resultSetMetaData.getTableName(i);
                    record.getTableNames()[i - 1] = tableName;
                    int columnType = resultSetMetaData.getColumnType(i);
                    record.getColumnTypes()[i - 1] = columnType;
                    Object payload = null;
                    switch (columnType) {
                      case Types.ARRAY:
                        payload = resultSet.getArray(i);
                        break;
                      case Types.BIGINT:
                        payload = resultSet.getInt(i);
                        break;
                      case Types.BIT:
                        payload = resultSet.getInt(i);
                        break;
                      case Types.BLOB:
                        payload = resultSet.getBlob(i);
                        break;
                      case Types.BOOLEAN:
                        payload = resultSet.getBoolean(i);
                        break;
                      case Types.CHAR:
                        payload = resultSet.getString(i);
                        break;
                      case Types.CLOB:
                        payload = resultSet.getClob(i);
                        break;
                      case Types.DATE:
                        payload = resultSet.getDate(i);
                        break;
                      case Types.DECIMAL:
                        payload = resultSet.getBigDecimal(i);
                        break;
                      case Types.DOUBLE:
                        payload = resultSet.getDouble(i);
                        break;
                      case Types.FLOAT:
                        payload = resultSet.getFloat(i);
                        break;
                      case Types.INTEGER:
                        payload = resultSet.getInt(i);
                        break;
                      case Types.LONGNVARCHAR:
                        payload = resultSet.getString(i);
                        break;
                      case Types.LONGVARCHAR:
                        payload = resultSet.getString(i);
                        break;
                      case Types.NCHAR:
                        payload = resultSet.getNString(i);
                        break;
                      case Types.NCLOB:
                        payload = resultSet.getNClob(i);
                        break;
                      case Types.SMALLINT:
                        payload = resultSet.getInt(i);
                        break;
                      case Types.TIME:
                        payload = resultSet.getTime(i);
                        break;
                      case Types.TIMESTAMP:
                        payload = resultSet.getTimestamp(i);
                        break;
                      case Types.TINYINT:
                        payload = resultSet.getInt(i);
                        break;
                      case Types.VARCHAR:
                        payload = resultSet.getString(i);
                        break;
                      default:
                        payload = resultSet.getObject(i);
                        break;
                    }
                    record.getColumnValues()[i - 1] = payload;
                  }
                } catch (Exception e) {
                  LOGGER.error("Can't map row", e);
                }
                return record;
              }
            })).setCoder(SerializableCoder.of(JdbcDataRecord.class));

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

  private class InsertRecord {

    private int columnType;
    private Object columnValue;

    public InsertRecord(int columnType, Object columnValue) {
      this.columnType = columnType;
      this.columnValue = columnValue;
    }

    public int getColumnType() {
      return columnType;
    }

    public void setColumnType(int columnType) {
      this.columnType = columnType;
    }

    public Object getColumnValue() {
      return columnValue;
    }

    public void setColumnValue(Object columnValue) {
      this.columnValue = columnValue;
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    ArrayList<KV<Integer, String>> data = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      KV<Integer, String> kv = KV.of(i, "TEST");
      data.add(kv);
    }
    pipeline.apply(Create.of(data))
        .apply(JdbcIO.write().withDataSource(dataSource)
            .withQuery("insert into TEST values(?, ?)")
            .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
              public void setParameters(KV<Integer, String> element, PreparedStatement statement)
                  throws Exception {
                statement.setInt(1, element.getKey());
                statement.setString(2, element.getValue());
              }
            }));

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
