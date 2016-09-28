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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>IO to read and write data on JDBC.</p>
 * <p>
 * <h3>Reading from JDBC datasource</h3>
 * <p>
 * JdbcIO source returns a bounded collection of {@code T} as a
 * {@code PCollection<T>}. T is the type returned by the provided {@link RowMapper}.
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
 *   .withQuery("select id,name from Person")
 *   .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       KV<Integer, String> kv = KV.of(resultSet.getInt(1), resultSet.getString(2));
 *       return kv;
 *     }
 *   })
 *
 *   }
 * </pre>
 * <h3>Writing to JDBC datasource</h3>
 * <p>
 * JDBC sink supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting each T into a {@link PreparedStatement} via a user-provided
 * {@link PreparedStatementSetter}.
 * </p>
 * <p>
 * Like the source, to configure JDBC sink, you have to provide a datasource. For instance:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.write()
 *      .withDataSource(myDataSource)
 *      .withQuery("insert into Person values(?, ?)")
 *      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
 *        public void setParameters(KV<Integer, String> element, PreparedStatement query) {
 *          // use the PCollection element to set parameters of the SQL query used to insert
 *          // in the database
 *          // for instance:
 *          query.setInt(1, kv.getKey());
 *          query.setString(2, kv.getValue());
 *        }
 *      })
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
  public static Read<?> read() {
    return new Read(new DataSourceConfiguration(null, null, null, null, 10, null), null, null);
  }

  /**
   * Write data to a JDBC datasource.
   *
   * @return a {@link Write} {@link PTransform}.
   */
  public static Write<?> write() {
    return new Write(new DataSourceConfiguration(null, null, null, null, 10, null), null, null,
        1024L);
  }

  private JdbcIO() {
  }

  /**
   * An interface used by the JdbcIO Read for mapping rows of a ResultSet on a per-row basis.
   * Implementations of this interface perform the actual work of mapping each row to a result
   * object used in the {@link PCollection}.
   */
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  /**
   * A POJO describing a {@link DataSource}, either providing directly a {@link DataSource} or all
   * properties allowing to create a {@link DataSource}.
   * Warning: if you provide directly a {@link DataSource} (using {@code withDataSource() method)},
   * this {@link DataSource} has to be {@link Serializable}.
   */
  public static class DataSourceConfiguration implements Serializable {

    @Nullable
    private final String driverClassName;
    @Nullable
    private final String url;
    @Nullable
    private final String username;
    @Nullable
    private final String password;
    private int initialSize;
    @Nullable
    private final DataSource dataSource;

    private DataSourceConfiguration(String driverClassName, String url, String username,
                                    String password, int initialSize, DataSource dataSource) {
      this.driverClassName = driverClassName;
      this.url = url;
      this.username = username;
      this.password = password;
      this.initialSize = initialSize;
      this.dataSource = dataSource;
    }

    public DataSourceConfiguration withDriverClassName(String driverClassName) {
      return new DataSourceConfiguration(driverClassName, url, username, password, initialSize,
          dataSource);
    }

    public DataSourceConfiguration withUrl(String url) {
      return new DataSourceConfiguration(driverClassName, url, username, password, initialSize,
          dataSource);
    }

    public DataSourceConfiguration withUsername(String username) {
      return new DataSourceConfiguration(driverClassName, url, username, password, initialSize,
          dataSource);
    }

    public DataSourceConfiguration withPassword(String password) {
      return new DataSourceConfiguration(driverClassName, url, username, password, initialSize,
          dataSource);
    }

    public DataSourceConfiguration withDataSource(DataSource dataSource) {
      return new DataSourceConfiguration(driverClassName, url, username, password, initialSize,
          dataSource);
    }

    public void validate() {
      if (dataSource == null) {
        Preconditions.checkNotNull("driverClassName", driverClassName);
        Preconditions.checkNotNull("url", url);
      }
      if (driverClassName == null || url == null) {
        Preconditions.checkNotNull("dataSource", dataSource);
      }
    }

    public void populateDisplayData(DisplayData.Builder builder) {
      if (dataSource != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", dataSource.getClass().getName()));
      }
      builder.addIfNotNull(DisplayData.item("driverClassName", driverClassName));
      builder.addIfNotNull(DisplayData.item("url", url));
      builder.addIfNotNull(DisplayData.item("username", username));
      builder.add(DisplayData.item("initialSize", initialSize));
    }

    public DataSource getDataSource() {
      if (dataSource != null) {
        return dataSource;
      } else {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(driverClassName);
        basicDataSource.setUrl(url);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
        basicDataSource.setInitialSize(initialSize);
        return basicDataSource;
      }
    }

  }

  /**
   * A {@link PTransform} to read data from a JDBC datasource.
   */
  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    public Read<T> withDriverClassName(String driverClassName) {
      return new Read<>(dataSourceConfiguration.withDriverClassName(driverClassName),
          query, rowMapper);
    }

    public Read<T> withUrl(String url) {
      return new Read<>(dataSourceConfiguration.withUrl(url), query, rowMapper);
    }

    public Read<T> withDataSource(DataSource dataSource) {
      return new Read<>(dataSourceConfiguration.withDataSource(dataSource),
          query, rowMapper);
    }

    public Read<T> withQuery(String query) {
      return new Read<>(dataSourceConfiguration, query, rowMapper);
    }

    public <X> Read<X> withRowMapper(RowMapper<X> rowMapper) {
      return new Read<>(dataSourceConfiguration, query, rowMapper);
    }

    public Read<T> withUsername(String username) {
      return new Read<>(dataSourceConfiguration.withUsername(username), query, rowMapper);
    }

    public Read<T> withPassword(String password) {
      return new Read<>(dataSourceConfiguration.withPassword(password), query, rowMapper);
    }

    private final DataSourceConfiguration dataSourceConfiguration;
    private final String query;
    private final RowMapper<T> rowMapper;

    private Read(DataSourceConfiguration dataSourceConfiguration,
                 String query, RowMapper<T> rowMapper) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.query = query;
      this.rowMapper = rowMapper;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      PCollection<T> output = input.apply(Create.of(query))
          .apply(ParDo.of(new ReadFn<>(dataSourceConfiguration, rowMapper)));

      return output;
    }

    @Override
    public void validate(PBegin input) {
      Preconditions.checkNotNull(query, "query");
      Preconditions.checkNotNull(rowMapper, "rowMapper");
      dataSourceConfiguration.validate();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", query));
      builder.add(DisplayData.item("rowMapper", rowMapper.getClass().getName()));
      dataSourceConfiguration.populateDisplayData(builder);
    }


    /**
     * A {@link DoFn} executing the SQL query to read from the database.
     */
    static class ReadFn<T> extends DoFn<String, T> {

      private final DataSourceConfiguration dataSourceConfiguration;
      private final RowMapper<T> rowMapper;

      private DataSource dataSource;

      private ReadFn(DataSourceConfiguration dataSourceConfiguration,
                     RowMapper<T> rowMapper) {
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.rowMapper = rowMapper;
      }

      @Setup
      public void setup() throws Exception {
        dataSource = dataSourceConfiguration.getDataSource();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String query = context.element();

        try (Connection connection = (dataSourceConfiguration.username != null)
            ? dataSource.getConnection(dataSourceConfiguration.username,
                dataSourceConfiguration.password)
            : dataSource.getConnection()) {

          try (PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
              while (resultSet.next()) {
                T record = rowMapper.mapRow(resultSet);
                context.output(record);
              }
            }
          }
        }
      }
    }
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  public interface PreparedStatementSetter<T> extends Serializable {
    void setParameters(T element, PreparedStatement preparedStatement) throws Exception;
  }

  /**
   * A {@link PTransform} to write to a JDBC datasource.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {

    public Write<T> withDriverClassName(String driverClassName) {
      return new Write<>(dataSourceConfiguration.withDriverClassName(driverClassName), statement,
          preparedStatementSetter, batchSize);
    }

    public Write<T> withUrl(String url) {
      return new Write<>(dataSourceConfiguration.withUrl(url), statement,
          preparedStatementSetter, batchSize);
    }

    public Write<T> withUsername(String username) {
      return new Write<>(dataSourceConfiguration.withUsername(username), statement,
          preparedStatementSetter, batchSize);
    }

    public Write<T> withPassword(String password) {
      return new Write<>(dataSourceConfiguration.withPassword(password), statement,
          preparedStatementSetter, batchSize);
    }

    public Write<T> withDataSource(DataSource dataSource) {
      return new Write<>(dataSourceConfiguration.withDataSource(dataSource), statement,
          preparedStatementSetter, batchSize);
    }

    public Write<T> withStatement(String statement) {
      return new Write<>(dataSourceConfiguration, statement, preparedStatementSetter, batchSize);
    }

    public <X> Write<X> withPreparedStatementSetter(
        PreparedStatementSetter<X> preparedStatementSetter) {
      return new Write<>(dataSourceConfiguration, statement, preparedStatementSetter, batchSize);
    }

    public Write<T> withBatchSize(long batchSize) {
      return new Write<>(dataSourceConfiguration, statement, preparedStatementSetter, batchSize);
    }

    private final DataSourceConfiguration dataSourceConfiguration;
    private final String statement;
    private final PreparedStatementSetter<T> preparedStatementSetter;
    private final long batchSize;

    private Write(DataSourceConfiguration dataSourceConfiguration, String statement,
                  PreparedStatementSetter<T> preparedStatementSetter, long batchSize) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.statement = statement;
      this.preparedStatementSetter = preparedStatementSetter;
      this.batchSize = batchSize;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      input.apply(
          ParDo.of(new WriteFn<T>(dataSourceConfiguration, statement, preparedStatementSetter,
              batchSize)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<T> input) {
      dataSourceConfiguration.validate();
      Preconditions.checkNotNull("statement", statement);
      Preconditions.checkNotNull("preparedStatementSetter", preparedStatementSetter);
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final DataSourceConfiguration dataSourceConfiguration;
      private final String statement;
      private final PreparedStatementSetter<T> preparedStatementSetter;
      private long batchSize;

      private DataSource dataSource;
      private Connection connection;
      private PreparedStatement preparedStatement;
      private long batchCount;

      public WriteFn(DataSourceConfiguration dataSourceConfiguration, String statement,
                     PreparedStatementSetter<T> preparedStatementSetter, long batchSize) {
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.statement = statement;
        this.preparedStatementSetter = preparedStatementSetter;
        this.batchSize = batchSize;
      }

      @Setup
      public void setup() throws Exception {
        dataSource = dataSourceConfiguration.getDataSource();
        if (dataSourceConfiguration.username != null) {
          connection = dataSource.getConnection(dataSourceConfiguration.username,
              dataSourceConfiguration.password);
        } else {
          connection = dataSource.getConnection();
        }
        connection.setAutoCommit(false);
        preparedStatement = connection.prepareStatement(statement);
      }

      @StartBundle
      public void startBundle(Context context) {
        batchCount = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        T record = context.element();

        preparedStatement.clearParameters();
        preparedStatementSetter.setParameters(record, preparedStatement);
        preparedStatement.addBatch();

        batchCount++;

        if (batchCount >= batchSize) {
          finishBundle(context);
        }
      }

      @FinishBundle
      public void finishBundle(Context context) throws Exception {
        preparedStatement.executeBatch();
        connection.commit();
        batchCount = 0;
      }

      @Teardown
      public void closeConnection() throws Exception {
        if (preparedStatement != null) {
          preparedStatement.close();
        }
        if (connection != null) {
          connection.close();
        }
      }

    }

  }

}
