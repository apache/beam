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
 * <h3>Reading from JDBC datasource</h3>
 * <p>
 * JdbcIO source returns a bounded collection of {@code T} as a
 * {@code PCollection<T>}. T is the type returned by the provided {@link RowMapper}.
 * </p>
 * <p>
 * To configure the JDBC source, you have to provide a {@link DataSource} using {@code
 * withDataSource()} method (NB: the provided {@link DataSource} has to be {@link Serializable}) or
 * the configuration required to create a {@link DataSource}: JDBC driver class name (using
 * {@code withJdbcDriverClassName()} method) and JDBC URL (using {@code withJdbcUrl()} method).
 * Using this configuration, the {@link JdbcIO} will create the {@link DataSource} for you.
 * The username and password to connect to the database are optionals.
 * The following example illustrates how to configure a JDBC source to use MySQL database:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline.apply(JdbcIO.read()
 *   .withJdbcDriverClassName("com.mysql.jdbc.Driver")
 *   .withJdbcUrl("jdbc:mysql://hostname:3306/mydb")
 *   .withUsername("username")
 *   .withPassword("password")
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
 * <p>
 *   Optionally, you can provide the connection pool configuration using
 *   {@code withConnectionPoolInitialSize()} for the initial size of the connection pool,
 *   {@code withConnectionPoolMaxTotal()} for the max total connections count in the pool,
 *   {@code withConnectionPoolMinIdle()} for the min number of idle connections in the pool,
 *   {@code withConnectionPoolMaxIdle()} for the max number of idle connections in the pool.
 * </p>
 * <h3>Writing to JDBC datasource</h3>
 * <p>
 * JDBC sink supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting each T into a {@link PreparedStatement} via a user-provided
 * {@link PreparedStatementSetter}.
 * </p>
 * <p>
 * Like the source, to configure JDBC sink, you have to provide a {@link DataSource} or the
 * datasource configuration. For instance:
 * </p>
 * <pre>
 *   {@code
 *
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.write()
 *      .withJdbcDriverClassName("com.mysql.jdbc.Driver")
 *      .withJdbcUrl("jdbc:mysql://hostname:3306/mydb")
 *      .withUsername("username")
 *      .withPassword("password")
 *      .withStatement("insert into Person values(?, ?)")
 *      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
 *        public void setParameters(KV<Integer, String> element, PreparedStatement query) {
 *          query.setInt(1, kv.getKey());
 *          query.setString(2, kv.getValue());
 *        }
 *      })
 *
 *   }
 * </pre>
 * <p>
 *   The {@link JdbcIO} write accepts connection pool configuration using the same methods as in
 *   the read.
 * </p>
 * <p>
 *   NB: in case of failure, {@link JdbcIO} will retry to insert data. It means that the statement
 *   may be executed multiple times. An {@code INSERT} statement will duplicate the record in the
 *   database. If you want to avoid duplicate records, you can use {@code MERGE} (also named
 *   {@code upsert}). Take a look on
 *   <a href="https://en.wikipedia.org/wiki/Merge_(SQL)">https://en.wikipedia.org/wiki/Merge_(SQL)
 *   </a> for details.
 * </p>
 */
public class JdbcIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @return a {@link Read} {@link PTransform}.
   */
  public static Read<?> read() {
    return new Read(new DataSourceConfiguration(null, null, null, null, 5, 8, 0, 8, null), null,
        null);
  }

  /**
   * Write data to a JDBC datasource.
   *
   * @return a {@link Write} {@link PTransform}.
   */
  public static Write<?> write() {
    return new Write(new DataSourceConfiguration(null, null, null, null, 5, 8, 0, 8, null), null,
        null,
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
  static class DataSourceConfiguration implements Serializable {

    @Nullable
    private final String jdbcDriverClassName;
    @Nullable
    private final String jdbcUrl;
    @Nullable
    private final String username;
    @Nullable
    private final String password;
    private int connectionPoolInitialSize;
    private int connectionPoolMaxTotal;
    private int connectionPoolMinIdle;
    private int connectionPoolMaxIdle;
    @Nullable
    private final DataSource dataSource;

    private DataSourceConfiguration(String jdbcDriverClassName, String jdbcUrl, String username,
                                    String password, int connectionPoolInitialSize,
                                    int connectionPoolMaxTotal, int connectionPoolMinIdle,
                                    int connectionPoolMaxIdle, DataSource dataSource) {
      this.jdbcDriverClassName = jdbcDriverClassName;
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
      this.connectionPoolInitialSize = connectionPoolInitialSize;
      this.connectionPoolMaxTotal = connectionPoolMaxTotal;
      this.connectionPoolMinIdle = connectionPoolMinIdle;
      this.connectionPoolMaxIdle = connectionPoolMaxIdle;
      this.dataSource = dataSource;
    }

    public DataSourceConfiguration withJdbcDriverClassName(String jdbcDriverClassName) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withJdbcUrl(String jdbcUrl) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withUsername(String username) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withPassword(String password) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withConnectionPoolInitialSize(int connectionPoolInitialSize) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withConnectionPoolMaxTotal(int connectionPoolMaxTotal) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withConnectionPoolMinIdle(int connectionPoolMinIdle) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withConnectionPoolMaxIdle(int connectionPoolMaxIdle) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public DataSourceConfiguration withDataSource(DataSource dataSource) {
      return new DataSourceConfiguration(jdbcDriverClassName, jdbcUrl, username, password,
          connectionPoolInitialSize, connectionPoolMaxTotal, connectionPoolMinIdle,
          connectionPoolMaxIdle, dataSource);
    }

    public void validate() {
      if (dataSource == null) {
        Preconditions.checkNotNull(jdbcDriverClassName, "jdbcDriverClassName");
        Preconditions.checkNotNull(jdbcUrl, "jdbcUrl");
      }
      if (jdbcDriverClassName == null || jdbcUrl == null) {
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkArgument(dataSource instanceof Serializable,
            "dataSource is not serializable");
      }
    }

    public void populateDisplayData(DisplayData.Builder builder) {
      if (dataSource != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", dataSource.getClass().getName()));
      }
      builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", jdbcDriverClassName));
      builder.addIfNotNull(DisplayData.item("jdbcUrl", jdbcUrl));
      builder.addIfNotNull(DisplayData.item("username", username));
      builder.add(DisplayData.item("connectionPoolInitialSize", connectionPoolInitialSize));
      builder.add(DisplayData.item("connectionPoolMaxTotal", connectionPoolMaxTotal));
      builder.add(DisplayData.item("connectionPoolMinIdle", connectionPoolMinIdle));
      builder.add(DisplayData.item("connectionPoolMaxIdle", connectionPoolMaxIdle));
    }

    public DataSource getDataSource() {
      if (dataSource != null) {
        return dataSource;
      } else {
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(jdbcDriverClassName);
        basicDataSource.setUrl(jdbcUrl);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
        basicDataSource.setInitialSize(connectionPoolInitialSize);
        basicDataSource.setMaxTotal(connectionPoolMaxTotal);
        basicDataSource.setMinIdle(connectionPoolMinIdle);
        basicDataSource.setMaxIdle(connectionPoolMaxIdle);
        return basicDataSource;
      }
    }

    public Connection getConnection() throws Exception {
      DataSource ds = this.getDataSource();
      Connection connection = null;
      if (username != null) {
        connection = ds.getConnection(username, password);
      } else {
        connection = ds.getConnection();
      }
      return connection;
    }

  }

  /**
   * A {@link PTransform} to read data from a JDBC datasource.
   */
  public static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    public Read<T> withJdbcDriverClassName(String jdbcDriverClassName) {
      return new Read<>(dataSourceConfiguration.withJdbcDriverClassName(jdbcDriverClassName),
          query, rowMapper);
    }

    public Read<T> withJdbcUrl(String jdbcUrl) {
      return new Read<>(dataSourceConfiguration.withJdbcUrl(jdbcUrl), query, rowMapper);
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

        try (Connection connection = dataSourceConfiguration.getConnection()) {
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

    public Write<T> withJdbcDriverClassName(String jdbcDriverClassName) {
      return new Write<>(dataSourceConfiguration.withJdbcDriverClassName(jdbcDriverClassName),
          statement, preparedStatementSetter, batchSize);
    }

    public Write<T> withJdbcUrl(String jdbcUrl) {
      return new Write<>(dataSourceConfiguration.withJdbcUrl(jdbcUrl), statement,
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

    public Write<T> withConnectionPoolInitialSize(int connectionPoolInitialSize) {
      return new Write<>(
          dataSourceConfiguration.withConnectionPoolInitialSize(connectionPoolInitialSize),
          statement, preparedStatementSetter, batchSize);
    }

    public Write<T> withConnectionPoolMaxTotal(int connectionPoolMaxTotal) {
      return new Write<>(dataSourceConfiguration.withConnectionPoolMaxTotal(connectionPoolMaxTotal),
          statement, preparedStatementSetter, batchSize);
    }

    public Write<T> withConnectionPoolMinIdle(int connectionPoolMinIdle) {
      return new Write<>(dataSourceConfiguration.withConnectionPoolMinIdle(connectionPoolMinIdle),
          statement, preparedStatementSetter, batchSize);
    }

    public Write<T> withConnectionPoolMaxIdle(int connectionPoolMaxIdle) {
      return new Write<>(dataSourceConfiguration.withConnectionPoolMaxIdle(connectionPoolMaxIdle),
          statement, preparedStatementSetter, batchSize);
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
      Preconditions.checkNotNull(statement, "statement");
      Preconditions.checkNotNull(preparedStatementSetter, "preparedStatementSetter");
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final DataSourceConfiguration dataSourceConfiguration;
      private final String statement;
      private final PreparedStatementSetter<T> preparedStatementSetter;
      private final long batchSize;

      private transient Connection connection;
      private transient PreparedStatement preparedStatement;
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
        connection = dataSourceConfiguration.getConnection();
        connection.setAutoCommit(false);
        System.out.println(statement);
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
      public void teardown() throws Exception {
        try {
          if (preparedStatement != null) {
            preparedStatement.close();
          }
        } finally {
          if (connection != null) {
            connection.close();
          }
        }
      }
    }
  }

}
