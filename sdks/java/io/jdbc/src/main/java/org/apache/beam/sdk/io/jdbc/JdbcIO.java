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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on JDBC.
 * <p>
 * <h3>Reading from JDBC datasource</h3>
 * <p>
 * JdbcIO source returns a bounded collection of {@link JdbcDataRecord} as a
 * {@code PCollection<JdbcDataRecord>}.
 * </p>
 * <p>
 * {@link JdbcDataRecord} contains table name, column name, column type and column value for
 * each record.
 * </p>
 * <p>
 * To configure the JDBC source, you have to provide a {@link DataSource}. The username and
 * password to connect to the database are optionals. The following example illustrates how to
 * configure a JDBC source:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline.apply(JdbcIO.read()
 *   .withDataSource(myDataSource)
 *
 *   }
 * </pre>
 * <h3>Writing to JDBC datasource</h3>
 * <p>
 * JDBC sink supports writing records into a database. It expects a
 * {@code PCollection<JdbcDataRecord>}, converts the {@link JdbcDataRecord}s as SQL statement
 * and insert into the database.
 * </p>
 * <p>
 * Like the source, to configure JDBC sink, you have to provide a datasource. For instance:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.write().withDataSource(myDataSource)
 *
 *   }
 * </pre>
 */
public class JdbcIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @return a {@link Read} {@link PTransform}.
   */
  public static Read read() {
    return new Read(new Read.JdbcOptions(null, null, null, null));
  }

  /**
   * Write data to a JDBC datasource.
   *
   * @return a {@link Write} {@link PTransform}.
   */
  public static Write write() {
    return new Write(new Write.JdbcWriter(null, null, null));
  }

  private JdbcIO() {
  }

  /**
   * A {@link PTransform} to read data from a JDBC datasource.
   */
  public static class Read extends PTransform<PBegin, PCollection<JdbcDataRecord>> {

    public Read withDataSource(DataSource dataSource) {
      return new Read(options.withDataSource(dataSource));
    }

    public Read withQuery(String query) {
      return new Read(options.withQuery(query));
    }

    public Read withUsername(String username) {
      return new Read(options.withUsername(username));
    }

    public Read withPassword(String password) {
      return new Read(options.withPassword(password));
    }

    private final JdbcOptions options;

    private Read(JdbcOptions options) {
      this.options = options;
    }

    @Override
    public PCollection<JdbcDataRecord> apply(PBegin input) {
      PCollection<JdbcDataRecord> output = input.apply(Create.of(options))
          .apply(ParDo.of(new ReadFn()));

      return output;
    }

    @Override
    public void validate(PBegin input) {
      options.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      options.populateDisplayData(builder);
    }

    @VisibleForTesting
    static class JdbcOptions implements Serializable {

      private final DataSource dataSource;
      private final String query;
      @Nullable
      private final String username;
      @Nullable
      private final String password;

      private JdbcOptions(DataSource dataSource, String query, @Nullable String username,
                          @Nullable String password) {
        this.dataSource = dataSource;
        this.query = query;
        this.username = username;
        this.password = password;
      }

      public JdbcOptions withDataSource(DataSource dataSource) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withQuery(String query) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withUsername(String username) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public JdbcOptions withPassword(String password) {
        return new JdbcOptions(dataSource, query, username, password);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(query, "query");
      }

      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("dataSource", dataSource.getClass().getName()));
        builder.add(DisplayData.item("query", query));
        builder.addIfNotNull(DisplayData.item("username", username));
      }

      public DataSource getDataSource() {
        return dataSource;
      }

      public String getQuery() {
        return query;
      }

      @Nullable
      public String getUsername() {
        return username;
      }

      @Nullable
      public String getPassword() {
        return password;
      }
    }

    public static class ReadFn extends DoFn<JdbcOptions, JdbcDataRecord> {

      private ReadFn() {
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        JdbcOptions options = context.element();

        try (Connection connection = (options.getUsername() != null)
            ? options.getDataSource().getConnection(options.getUsername(), options.getPassword())
            : options.getDataSource().getConnection()) {

          try (PreparedStatement statement = connection.prepareStatement(options.getQuery())) {
            try (ResultSet resultSet = statement.executeQuery()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                JdbcDataRecord record = new JdbcDataRecord(resultSetMetaData.getColumnCount());

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
                context.output(record);
              }
            }
          }
        }
      }
    }
  }

  /**
   * A {@link PTransform} to write to a JDBC datasource.
   */
  public static class Write extends PTransform<PCollection<JdbcDataRecord>, PDone> {

    public Write withDataSource(DataSource dataSource) {
      return new Write(writer.withDataSource(dataSource));
    }

    public Write withUsername(String username) {
      return new Write(writer.withUsername(username));
    }

    public Write withPassword(String password) {
      return new Write(writer.withPassword(password));
    }

    private final JdbcWriter writer;

    private Write(JdbcWriter writer) {
      this.writer = writer;
    }

    @Override
    public PDone apply(PCollection<JdbcDataRecord> input) {
      input.apply(ParDo.of(writer));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<JdbcDataRecord> input) {
      writer.validate();
    }

    private static class JdbcWriter extends DoFn<JdbcDataRecord, Void> {

      private final DataSource dataSource;
      private final String username;
      private final String password;

      private Connection connection;

      public JdbcWriter(DataSource dataSource, String username, String password) {
        this.dataSource = dataSource;
        this.username = username;
        this.password = password;
      }

      public JdbcWriter withDataSource(DataSource dataSource) {
        return new JdbcWriter(dataSource, username, password);
      }

      public JdbcWriter withUsername(String username) {
        return new JdbcWriter(dataSource, username, password);
      }

      public JdbcWriter withPassword(String password) {
        return new JdbcWriter(dataSource, username, password);
      }

      public void validate() {
        Preconditions.checkNotNull(dataSource, "dataSource");
      }

      @Setup
      public void connectToDatabase() throws Exception {
        if (username != null) {
          connection = dataSource.getConnection(username, password);
        } else {
          connection = dataSource.getConnection();
        }
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        JdbcDataRecord record = context.element();
        // map record per table
        Map<String, List<InsertRecord>> tableMap = new HashMap<>();
        Map<String, String> insertPerTable = new HashMap<>();
        for (int i = 0; i < record.getTableNames().length; i++) {
          String tableName = record.getTableNames()[i];
          List<InsertRecord> recordList = tableMap.get(tableName);
          if (recordList == null) {
            recordList = new ArrayList<>();
          }
          recordList.add(
              new InsertRecord(
                  record.getColumnTypes()[i],
                  record.getColumnValues()[i]));
          tableMap.put(tableName, recordList);
        }
        // create insert string
        for (String tableName : tableMap.keySet()) {
          String insertString = "insert into " + tableName + " values(";
          for (InsertRecord insertRecord : tableMap.get(tableName)) {
            insertString = insertString + "?,";
          }
          // remove trailing ',' and close parentheses
          insertString = insertString.substring(0, insertString.length() - 1) + ")";
          LOGGER.debug(insertString);
          try {
            PreparedStatement statement = connection.prepareStatement(insertString);
            int index = 1;
            for (InsertRecord insertRecord : tableMap.get(tableName)) {
              switch (insertRecord.getColumnType()) {
                case Types.ARRAY:
                  statement.setArray(index, (Array) insertRecord.getColumnValue());
                  break;
                case Types.BIGINT:
                  statement.setInt(index, (int) insertRecord.getColumnValue());
                  break;
                case Types.BIT:
                  statement.setInt(index, (int) insertRecord.getColumnValue());
                  break;
                case Types.BLOB:
                  statement.setBlob(index, (Blob) insertRecord.getColumnValue());
                  break;
                case Types.BOOLEAN:
                  statement.setBoolean(index, (boolean) insertRecord.getColumnValue());
                  break;
                case Types.CHAR:
                  statement.setString(index, (String) insertRecord.getColumnValue());
                  break;
                case Types.CLOB:
                  statement.setClob(index, (Clob) insertRecord.getColumnValue());
                  break;
                case Types.DATE:
                  statement.setDate(index, (Date) insertRecord.getColumnValue());
                  break;
                case Types.DECIMAL:
                  statement.setBigDecimal(index, (BigDecimal) insertRecord.getColumnValue());
                  break;
                case Types.DOUBLE:
                  statement.setDouble(index, (double) insertRecord.getColumnValue());
                  break;
                case Types.FLOAT:
                  statement.setFloat(index, (float) insertRecord.getColumnValue());
                  break;
                case Types.INTEGER:
                  statement.setInt(index, (int) insertRecord.getColumnValue());
                  break;
                case Types.LONGNVARCHAR:
                  statement.setString(index, (String) insertRecord.getColumnValue());
                  break;
                case Types.LONGVARCHAR:
                  statement.setString(index, (String) insertRecord.getColumnValue());
                  break;
                case Types.NCHAR:
                  statement.setNString(index, (String) insertRecord.getColumnValue());
                  break;
                case Types.NCLOB:
                  statement.setNClob(index, (NClob) insertRecord.getColumnValue());
                  break;
                case Types.SMALLINT:
                  statement.setInt(index, (int) insertRecord.getColumnValue());
                  break;
                case Types.TIME:
                  statement.setTime(index, (Time) insertRecord.getColumnValue());
                  break;
                case Types.TIMESTAMP:
                  statement.setTimestamp(index, (Timestamp) insertRecord.getColumnValue());
                  break;
                case Types.TINYINT:
                  statement.setInt(index, (int) insertRecord.getColumnValue());
                  break;
                case Types.VARCHAR:
                  statement.setString(index, (String) insertRecord.getColumnValue());
                  break;
                default:
                  statement.setObject(index, insertRecord.getColumnValue());
                  break;
              }
              index++;
            }
            statement.executeUpdate();
            statement.close();
          } catch (Exception e) {
            LOGGER.warn("Can't insert data into table", e);
          }
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        connection.commit();
      }

      @Teardown
      public void closeConnection() throws Exception {
        if (connection != null) {
          connection.close();
        }
      }

    }

    private static class InsertRecord {

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

  }

}
