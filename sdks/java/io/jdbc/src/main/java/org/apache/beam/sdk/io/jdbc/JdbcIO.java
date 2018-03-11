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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on JDBC.
 *
 * <h3>Reading from JDBC datasource</h3>
 *
 * <p>JdbcIO source returns a bounded collection of {@code T} as a {@code PCollection<T>}. T is the
 * type returned by the provided {@link RowMapper}.
 *
 * <p>To configure the JDBC source, you have to provide a {@link DataSourceConfiguration} using<br>
 * 1. {@link DataSourceConfiguration#create(DataSource)}(which must be {@link Serializable});<br>
 * 2. or {@link DataSourceConfiguration#create(String, String)}(driver class name and url).
 * Optionally, {@link DataSourceConfiguration#withUsername(String)} and
 * {@link DataSourceConfiguration#withPassword(String)} allows you to define username and password.
 *
 * <p>For example:
 * <pre>{@code
 * pipeline.apply(JdbcIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *          "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
 *        .withUsername("username")
 *        .withPassword("password"))
 *   .withQuery("select id,name from Person")
 *   .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
 *   .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * );
 * }</pre>
 *
 * <p>Query parameters can be configured using a user-provided {@link StatementPreparator}.
 * For example:</p>
 *
 * <pre>{@code
 * pipeline.apply(JdbcIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *       "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb",
 *       "username", "password"))
 *   .withQuery("select id,name from Person where name = ?")
 *   .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
 *   .withStatementPreparator(new JdbcIO.StatementPreparator() {
 *     public void setParameters(PreparedStatement preparedStatement) throws Exception {
 *       preparedStatement.setString(1, "Darwin");
 *     }
 *   })
 *   .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * );
 * }</pre>
 *
 * <h3>Writing to JDBC datasource</h3>
 *
 * <p>JDBC sink supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting each T into a {@link PreparedStatement} via a user-provided {@link
 * PreparedStatementSetter}.
 *
 * <p>Like the source, to configure the sink, you have to provide a {@link DataSourceConfiguration}.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(JdbcIO.<KV<Integer, String>>write()
 *      .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *            "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
 *          .withUsername("username")
 *          .withPassword("password"))
 *      .withStatement("insert into Person values(?, ?)")
 *      .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<Integer, String>>() {
 *        public void setParameters(KV<Integer, String> element, PreparedStatement query)
 *          throws SQLException {
 *          query.setInt(1, kv.getKey());
 *          query.setString(2, kv.getValue());
 *        }
 *      })
 *    );
 * }</pre>
 *
 * <p>NB: in case of transient failures, Beam runners may execute parts of JdbcIO.Write multiple
 * times for fault tolerance. Because of that, you should avoid using {@code INSERT} statements,
 * since that risks duplicating records in the database, or failing due to primary key conflicts.
 * Consider using <a href="https://en.wikipedia.org/wiki/Merge_(SQL)">MERGE ("upsert")
 * statements</a> supported by your database instead.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class JdbcIO {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_JdbcIO_Read.Builder<T>().build();
  }

  /**
   * Like {@link #read}, but executes multiple instances of the query substituting each element
   * of a {@link PCollection} as query parameters.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   * @param <OutputT> Type of the data to be read.
   */
  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_JdbcIO_ReadAll.Builder<ParameterT, OutputT>().build();
  }

  private static final long DEFAULT_BATCH_SIZE = 1000L;

  /**
   * Write data to a JDBC datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new AutoValue_JdbcIO_Write.Builder<T>()
            .setBatchSize(DEFAULT_BATCH_SIZE)
            .setRetryStrategy(new DefaultRetryStrategy())
            .build();
  }

  /**
   * This is the default {@link Predicate} we use to detect DeadLock.
   * It basically test if the {@link SQLException#getSQLState()} equals 40001.
   * 40001 is the SQL State used by most of database to identify deadlock.
   */
  public static class DefaultRetryStrategy implements RetryStrategy {
    @Override
    public boolean apply(SQLException e) {
      return (e.getSQLState().equals("40001"));
    }
  }

  private JdbcIO() {}

  /**
   * An interface used by {@link JdbcIO.Read} for converting each row of the {@link ResultSet} into
   * an element of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  /**
   * A POJO describing a {@link DataSource}, either providing directly a {@link DataSource} or all
   * properties allowing to create a {@link DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable abstract ValueProvider<String> getDriverClassName();
    @Nullable abstract ValueProvider<String> getUrl();
    @Nullable abstract ValueProvider<String> getUsername();
    @Nullable abstract ValueProvider<String> getPassword();
    @Nullable abstract ValueProvider<String> getConnectionProperties();
    abstract int getPoolMaxTotal();
    @Nullable abstract DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(ValueProvider<String> driverClassName);
      abstract Builder setUrl(ValueProvider<String> url);
      abstract Builder setUsername(ValueProvider<String> username);
      abstract Builder setPassword(ValueProvider<String> password);
      abstract Builder setConnectionProperties(ValueProvider<String> connectionProperties);
      abstract Builder setPoolMaxTotal(int poolMaxTotal);
      abstract Builder setDataSource(DataSource dataSource);
      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource != null, "dataSource can not be null");
      checkArgument(dataSource instanceof Serializable, "dataSource must be Serializable");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
              .setDataSource(dataSource)
              .setPoolMaxTotal(8)
              .build();
    }

    public static DataSourceConfiguration create(String driverClassName, String url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
              .setDriverClassName(ValueProvider.StaticValueProvider.of(driverClassName))
              .setUrl(ValueProvider.StaticValueProvider.of(url))
              .setPoolMaxTotal(8)
              .build();
    }

    public static DataSourceConfiguration create(ValueProvider<String> driverClassName,
                                                 ValueProvider<String> url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
              .setDriverClassName(driverClassName)
              .setUrl(url)
              .setPoolMaxTotal(8).build();
    }

    public DataSourceConfiguration withUsername(String username) {
      return builder().setUsername(ValueProvider.StaticValueProvider.of(username)).build();
    }

    public DataSourceConfiguration withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return builder().setPassword(ValueProvider.StaticValueProvider.of(password)).build();
    }

    public DataSourceConfiguration withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    /**
     * Sets the connection properties passed to driver.connect(...).
     * Format of the string must be [propertyName=property;]*
     *
     * <p>NOTE - The "user" and "password" properties can be add via {@link #withUsername(String)},
     * {@link #withPassword(String)}, so they do not need to be included here.
     */
    public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder()
              .setConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties))
              .build();
    }

    /**
     * Same as {@link #withConnectionProperties(String)} but accepting a ValueProvider.
     */
    public DataSourceConfiguration withConnectionProperties(
            ValueProvider<String> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    public DataSourceConfiguration withPoolMaxTotal(int poolMaxTotal) {
      return builder().setPoolMaxTotal(poolMaxTotal).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", getDriverClassName()));
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
      }
      builder.add(DisplayData.item("poolMaxTotal", getPoolMaxTotal()));
    }

    DataSource buildDatasource() throws Exception {
      DataSource current = null;
      if (getDataSource() != null) {
        current = getDataSource();
      } else {
        BasicDataSource basicDataSource = new BasicDataSource();
        if (getDriverClassName() != null) {
          basicDataSource.setDriverClassName(getDriverClassName().get());
        }
        if (getUrl() != null) {
          basicDataSource.setUrl(getUrl().get());
        }
        if (getUsername() != null) {
          basicDataSource.setUsername(getUsername().get());
        }
        if (getPassword() != null) {
          basicDataSource.setPassword(getPassword().get());
        }
        if (getConnectionProperties() != null && getConnectionProperties().get() != null) {
          basicDataSource.setConnectionProperties(getConnectionProperties().get());
        }
        current = basicDataSource;
      }

      // wrapping the datasource as a pooling datasource
      DataSourceConnectionFactory connectionFactory = new DataSourceConnectionFactory(current);
      PoolableConnectionFactory poolableConnectionFactory =
              new PoolableConnectionFactory(connectionFactory, null);
      GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
      poolConfig.setMaxTotal(getPoolMaxTotal());
      poolConfig.setBlockWhenExhausted(true);
      poolConfig.setMaxIdle(getPoolMaxTotal());
      poolConfig.setMinIdle(0);
      poolConfig.setTestOnBorrow(false);
      poolConfig.setTestOnReturn(false);
      poolConfig.setTestWhileIdle(true);
      poolConfig.setLifo(true);
      GenericObjectPool connectionPool =
              new GenericObjectPool(poolableConnectionFactory, poolConfig);
      poolableConnectionFactory.setPool(connectionPool);
      poolableConnectionFactory.setDefaultAutoCommit(false);
      poolableConnectionFactory.setDefaultReadOnly(false);
      PoolingDataSource poolingDataSource = new PoolingDataSource(connectionPool);
      return poolingDataSource;
    }

  }

  /**
   * An interface creating a {@link DataSource}. It's an alternative to
   * {@link DataSourceConfiguration}allowing user a complete control of
   * the created {@link DataSource}.
   */
  public interface DataSourceFactory extends Serializable {
    DataSource create() throws Exception;
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  @FunctionalInterface
  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable abstract DataSourceConfiguration getDataSourceConfiguration();
    @Nullable abstract DataSourceFactory getDataSourceFactory();
    @Nullable abstract ValueProvider<String> getQuery();
    @Nullable abstract StatementPreparator getStatementPreparator();
    @Nullable abstract RowMapper<T> getRowMapper();
    @Nullable abstract Coder<T> getCoder();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration config);
      abstract Builder<T> setDataSourceFactory(DataSourceFactory factory);
      abstract Builder<T> setQuery(ValueProvider<String> query);
      abstract Builder<T> setStatementPreparator(StatementPreparator statementPreparator);
      abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(DataSourceConfiguration configuration) {
      return toBuilder().setDataSourceConfiguration(configuration).build();
    }

    public Read<T> withDataSourceFactory(DataSourceFactory factory) {
      return toBuilder().setDataSourceFactory(factory).build();
    }

    public Read<T> withQuery(String query) {
      checkArgument(query != null, "query can not be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public Read<T> withQuery(ValueProvider<String> query) {
      checkArgument(query != null, "query can not be null");
      return toBuilder().setQuery(query).build();
    }

    public Read<T> withStatementPreparator(StatementPreparator statementPreparator) {
      checkArgument(statementPreparator != null, "statementPreparator can not be null");
      return toBuilder().setStatementPreparator(statementPreparator).build();
    }

    public Read<T> withRowMapper(RowMapper<T> rowMapper) {
      checkArgument(rowMapper != null, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(getQuery() != null, "withQuery() is required");
      checkArgument(getRowMapper() != null, "withRowMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument((getDataSourceConfiguration() != null || getDataSourceFactory() != null),
              "either withDataSourceConfiguration() or withDataSourceFactory() is required");
      checkArgument((getDataSourceConfiguration() == null || getDataSourceFactory() == null),
          "only withDataSourceConfiguration() or withDataSourceFactory() should be used");

      return input
          .apply(Create.of((Void) null))
          .apply(
              JdbcIO.<Void, T>readAll()
                  .withDataSourceConfiguration(getDataSourceConfiguration())
                  .withDataSourceFactory(getDataSourceFactory())
                  .withQuery(getQuery())
                  .withCoder(getCoder())
                  .withRowMapper(getRowMapper())
                  .withParameterSetter(
                      (element, preparedStatement) -> {
                        if (getStatementPreparator() != null) {
                          getStatementPreparator().setParameters(preparedStatement);
                        }
                      }));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceConfiguration() != null) {
        getDataSourceConfiguration().populateDisplayData(builder);
      }
      if (getDataSourceFactory() != null) {
        builder.add(DisplayData.item("factory",
                getDataSourceFactory().getClass().getName()));
      }
    }
  }

  /** Implementation of {@link #readAll}. */

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
          extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {
    @Nullable abstract DataSourceConfiguration getDataSourceConfiguration();
    @Nullable abstract DataSourceFactory getDataSourceFactory();
    @Nullable abstract ValueProvider<String> getQuery();
    @Nullable abstract PreparedStatementSetter<ParameterT> getParameterSetter();
    @Nullable abstract RowMapper<OutputT> getRowMapper();
    @Nullable abstract Coder<OutputT> getCoder();

    abstract Builder<ParameterT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDataSourceConfiguration(
              DataSourceConfiguration config);
      abstract Builder<ParameterT, OutputT> setDataSourceFactory(DataSourceFactory factory);
      abstract Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);
      abstract Builder<ParameterT, OutputT> setParameterSetter(
              PreparedStatementSetter<ParameterT> parameterSetter);
      abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);
      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);
      abstract ReadAll<ParameterT, OutputT> build();
    }

    public ReadAll<ParameterT, OutputT> withDataSourceConfiguration(
            DataSourceConfiguration configuration) {
      return toBuilder().setDataSourceConfiguration(configuration).build();
    }

    public ReadAll<ParameterT, OutputT> withDataSourceFactory(DataSourceFactory factory) {
      return toBuilder().setDataSourceFactory(factory).build();
    }

    public ReadAll<ParameterT, OutputT> withQuery(String query) {
      checkArgument(query != null, "JdbcIO.readAll().withQuery(query) called with null query");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public ReadAll<ParameterT, OutputT> withQuery(ValueProvider<String> query) {
      checkArgument(query != null, "JdbcIO.readAll().withQuery(query) called with null query");
      return toBuilder().setQuery(query).build();
    }

    public ReadAll<ParameterT, OutputT> withParameterSetter(
            PreparedStatementSetter<ParameterT> parameterSetter) {
      checkArgument(parameterSetter != null,
              "JdbcIO.readAll().withParameterSetter(parameterSetter) called "
                      + "with null statementPreparator");
      return toBuilder().setParameterSetter(parameterSetter).build();
    }

    public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
      checkArgument(rowMapper != null,
              "JdbcIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(coder != null, "JdbcIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      return input
          .apply(
              ParDo.of(
                  new ReadFn<>(
                      getDataSourceConfiguration(),
                      getDataSourceFactory(),
                      getQuery(),
                      getParameterSetter(),
                      getRowMapper())))
          .setCoder(getCoder())
          .apply(new Reparallelize<>());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceConfiguration() != null) {
        getDataSourceConfiguration().populateDisplayData(builder);
      }
      if (getDataSourceFactory() != null) {
        builder.add(DisplayData.item("factory",
                getDataSourceFactory().getClass().getName()));
      }
    }
  }

  /** A {@link DoFn} executing the SQL query to read from the database. */
  private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final DataSourceConfiguration dataSourceConfiguration;
    private final DataSourceFactory dataSourceFactory;
    private final ValueProvider<String> query;
    private final PreparedStatementSetter<ParameterT> parameterSetter;
    private final RowMapper<OutputT> rowMapper;

    private DataSource dataSource;
    private Connection connection;

    private ReadFn(
        DataSourceConfiguration dataSourceConfiguration,
        DataSourceFactory dataSourceFactory,
        ValueProvider<String> query,
        PreparedStatementSetter<ParameterT> parameterSetter,
        RowMapper<OutputT> rowMapper) {
      this.dataSourceConfiguration = dataSourceConfiguration;
      this.dataSourceFactory = dataSourceFactory;
      this.query = query;
      this.parameterSetter = parameterSetter;
      this.rowMapper = rowMapper;
    }

    @Setup
    public void setup() throws Exception {
      if (dataSourceFactory != null) {
        dataSource = dataSourceFactory.create();
      } else {
        dataSource = dataSourceConfiguration.buildDatasource();
      }
      connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      try (PreparedStatement statement = connection.prepareStatement(query.get())) {
        parameterSetter.setParameters(context.element(), statement);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            context.output(rowMapper.mapRow(resultSet));
          }
        }
      }
    }

    @Teardown
    public void teardown() throws Exception {
      connection.close();
      if (dataSource instanceof AutoCloseable) {
        ((AutoCloseable) dataSource).close();
      }
    }
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  @FunctionalInterface
  public interface PreparedStatementSetter<T> extends Serializable {
    void setParameters(T element, PreparedStatement preparedStatement) throws Exception;
  }

  /**
   * An interface used to control if we retry the statements when a {@link SQLException} occurs.
   * If {@link RetryStrategy#apply(SQLException)} returns true, {@link Write} tries
   * to replay the statements.
   */
  @FunctionalInterface
  public interface RetryStrategy extends Serializable {
    boolean apply(SQLException sqlException);
  }

  /** A {@link PTransform} to write to a JDBC datasource. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable abstract DataSourceConfiguration getDataSourceConfiguration();
    @Nullable abstract DataSourceFactory getDataSourceFactory();
    @Nullable abstract String getStatement();
    abstract long getBatchSize();
    @Nullable abstract PreparedStatementSetter<T> getPreparedStatementSetter();
    @Nullable abstract RetryStrategy getRetryStrategy();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration config);
      abstract Builder<T> setDataSourceFactory(DataSourceFactory factory);
      abstract Builder<T> setStatement(String statement);
      abstract Builder<T> setBatchSize(long batchSize);
      abstract Builder<T> setPreparedStatementSetter(PreparedStatementSetter<T> setter);
      abstract Builder<T> setRetryStrategy(RetryStrategy deadlockPredicate);

      abstract Write<T> build();
    }

    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return toBuilder().setDataSourceConfiguration(config).build();
    }
    public Write<T> withDataSourceFactory(DataSourceFactory factory) {
      return toBuilder().setDataSourceFactory(factory).build();
    }
    public Write<T> withStatement(String statement) {
      return toBuilder().setStatement(statement).build();
    }
    public Write<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return toBuilder().setPreparedStatementSetter(setter).build();
    }

    /**
     * Provide a maximum size in number of SQL statenebt for the batch. Default is 1000.
     *
     * @param batchSize maximum batch size in number of statements
     */
    public Write<T> withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %d", batchSize);
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryStrategy} to determine
     * if it will retry the statements.
     * If {@link RetryStrategy#apply(SQLException)} returns {@code true},
     * then {@link Write} retries the statements.
     */
    public Write<T> withRetryStrategy(RetryStrategy retryStrategy) {
      checkArgument(retryStrategy != null, "retryStrategy can not be null");
      return toBuilder().setRetryStrategy(retryStrategy).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(
              (getDataSourceConfiguration() != null || getDataSourceFactory() != null),
              "either withDataSourceConfiguration() or withDataSourceFactory() is required");
      checkArgument(
          (getDataSourceConfiguration() == null || getDataSourceFactory() == null),
          "only withDataSourceConfiguration() or withDataSourceFactory() is allowed");
      checkArgument(getStatement() != null, "withStatement() is required");
      checkArgument(
          getPreparedStatementSetter() != null, "withPreparedStatementSetter() is required");

      input.apply(ParDo.of(new WriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final Write<T> spec;

      private static final int MAX_RETRIES = 5;
      private static final FluentBackoff BUNDLE_WRITE_BACKOFF =
              FluentBackoff.DEFAULT
                      .withMaxRetries(MAX_RETRIES).withInitialBackoff(Duration.standardSeconds(5));

      private DataSource dataSource;
      private Connection connection;
      private PreparedStatement preparedStatement;
      private List<T> records = new ArrayList<>();

      public WriteFn(Write<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        if (spec.getDataSourceFactory() != null) {
          dataSource = spec.getDataSourceFactory().create();
        } else {
          dataSource = spec.getDataSourceConfiguration().buildDatasource();
        }
      }

      @StartBundle
      public void startBundle() throws Exception {
        connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        preparedStatement = connection.prepareStatement(spec.getStatement());
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        T record = context.element();

        records.add(record);

        if (records.size() >= spec.getBatchSize()) {
          executeBatch();
        }
      }

      private void processRecord(T record, PreparedStatement preparedStatement) {
        try {
          preparedStatement.clearParameters();
          spec.getPreparedStatementSetter().setParameters(record, preparedStatement);
          preparedStatement.addBatch();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        executeBatch();
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

      private void executeBatch() throws SQLException, IOException, InterruptedException {
        if (records.size() == 0) {
          return;
        }
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = BUNDLE_WRITE_BACKOFF.backoff();
        while (true) {
          try (PreparedStatement preparedStatement =
                       connection.prepareStatement(spec.getStatement())) {
            try {
              // add each record in the statement batch
              for (T record : records) {
                processRecord(record, preparedStatement);
              }
              // execute the batch
              preparedStatement.executeBatch();
              // commit the changes
              connection.commit();
              break;
            } catch (SQLException exception) {
              if (!spec.getRetryStrategy().apply(exception)) {
                throw exception;
              }
              LOG.warn("Deadlock detected, retrying", exception);
              // clean up the statement batch and the connection state
              preparedStatement.clearBatch();
              connection.rollback();
              if (!BackOffUtils.next(sleeper, backoff)) {
                // we tried the max number of times
                throw exception;
              }
            }
          }
        }
        records.clear();
      }

      @Teardown
      public void teardown() throws Exception {
        if (dataSource instanceof AutoCloseable) {
          ((AutoCloseable) dataSource).close();
        }
      }
    }
  }

  private static class Reparallelize<T> extends PTransform<PCollection<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      // See https://issues.apache.org/jira/browse/BEAM-2803
      // We use a combined approach to "break fusion" here:
      // (see https://cloud.google.com/dataflow/service/dataflow-service-desc#preventing-fusion)
      // 1) force the data to be materialized by passing it as a side input to an identity fn,
      // then 2) reshuffle it with a random key. Initial materialization provides some parallelism
      // and ensures that data to be shuffled can be generated in parallel, while reshuffling
      // provides perfect parallelism.
      // In most cases where a "fusion break" is needed, a simple reshuffle would be sufficient.
      // The current approach is necessary only to support the particular case of JdbcIO where
      // a single query may produce many gigabytes of query results.
      PCollectionView<Iterable<T>> empty =
          input
              .apply("Consume", Filter.by(SerializableFunctions.constant(false)))
              .apply(View.asIterable());
      PCollection<T> materialized =
          input.apply(
              "Identity",
              ParDo.of(
                      new DoFn<T, T>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                          c.output(c.element());
                        }
                      })
                  .withSideInputs(empty));
      return materialized.apply(Reshuffle.viaRandomKey());
    }
  }
}
