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

import static org.apache.beam.sdk.io.jdbc.SchemaUtil.checkNullabilityForFields;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * Optionally, {@link DataSourceConfiguration#withUsername(String)} and {@link
 * DataSourceConfiguration#withPassword(String)} allows you to define username and password.
 *
 * <p>For example:
 *
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
 * <p>Query parameters can be configured using a user-provided {@link StatementPreparator}. For
 * example:
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
 * <p>To customize the building of the {@link DataSource} we can provide a {@link
 * SerializableFunction}. For example if you need to provide a {@link PoolingDataSource} from an
 * existing {@link DataSourceConfiguration}: you can use a {@link PoolableDataSourceProvider}:
 *
 * <pre>{@code
 * pipeline.apply(JdbcIO.<KV<Integer, String>>read()
 *   .withDataSourceProviderFn(JdbcIO.PoolableDataSourceProvider.of(
 *       JdbcIO.DataSourceConfiguration.create(
 *           "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb",
 *           "username", "password")))
 *    // ...
 * );
 * }</pre>
 *
 * <p>By default, the provided function requests a DataSource per execution thread. In some
 * circumstances this can quickly overwhelm the database by requesting too many connections. In that
 * case you should look into sharing a single instance of a {@link PoolingDataSource} across all the
 * execution threads. For example:
 *
 * <pre>{@code
 * private static class MyDataSourceProviderFn implements SerializableFunction<Void, DataSource> {
 *   private static transient DataSource dataSource;
 *
 *   @Override
 *   public synchronized DataSource apply(Void input) {
 *     if (dataSource == null) {
 *       dataSource = ... build data source ...
 *     }
 *     return dataSource;
 *   }
 * }
 *
 * pipeline.apply(JdbcIO.<KV<Integer, String>>read()
 *   .withDataSourceProviderFn(new MyDataSourceProviderFn())
 *   // ...
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
 *          query.setInt(1, element.getKey());
 *          query.setString(2, element.getValue());
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
@Experimental(Kind.SOURCE_SINK)
public class JdbcIO {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcIO.class);

  /**
   * Read data from a JDBC datasource.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_JdbcIO_Read.Builder<T>()
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setOutputParallelization(true)
        .build();
  }

  /** Read Beam {@link Row}s from a JDBC data source. */
  @Experimental(Kind.SCHEMAS)
  public static ReadRows readRows() {
    return new AutoValue_JdbcIO_ReadRows.Builder()
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setOutputParallelization(true)
        .setStatementPreparator(ignored -> {})
        .build();
  }

  /**
   * Like {@link #read}, but executes multiple instances of the query substituting each element of a
   * {@link PCollection} as query parameters.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   * @param <OutputT> Type of the data to be read.
   */
  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_JdbcIO_ReadAll.Builder<ParameterT, OutputT>()
        .setFetchSize(DEFAULT_FETCH_SIZE)
        .setOutputParallelization(true)
        .build();
  }

  private static final long DEFAULT_BATCH_SIZE = 1000L;
  private static final int DEFAULT_FETCH_SIZE = 50_000;
  // Default values used from fluent backoff.
  private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.standardSeconds(1);
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardDays(1000);

  /**
   * Write data to a JDBC datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new Write();
  }

  public static <T> WriteVoid<T> writeVoid() {
    return new AutoValue_JdbcIO_WriteVoid.Builder<T>()
        .setBatchSize(DEFAULT_BATCH_SIZE)
        .setRetryStrategy(new DefaultRetryStrategy())
        .setRetryConfiguration(RetryConfiguration.create(5, null, Duration.standardSeconds(5)))
        .build();
  }

  /**
   * This is the default {@link Predicate} we use to detect DeadLock. It basically test if the
   * {@link SQLException#getSQLState()} equals 40001. 40001 is the SQL State used by most of
   * database to identify deadlock.
   */
  public static class DefaultRetryStrategy implements RetryStrategy {
    @Override
    public boolean apply(SQLException e) {
      return "40001".equals(e.getSQLState());
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

    abstract @Nullable ValueProvider<String> getDriverClassName();

    abstract @Nullable ValueProvider<String> getUrl();

    abstract @Nullable ValueProvider<String> getUsername();

    abstract @Nullable ValueProvider<String> getPassword();

    abstract @Nullable ValueProvider<String> getConnectionProperties();

    abstract @Nullable ValueProvider<Collection<String>> getConnectionInitSqls();

    abstract @Nullable DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(ValueProvider<String> driverClassName);

      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setConnectionProperties(ValueProvider<String> connectionProperties);

      abstract Builder setConnectionInitSqls(ValueProvider<Collection<String>> connectionInitSqls);

      abstract Builder setDataSource(DataSource dataSource);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource != null, "dataSource can not be null");
      checkArgument(dataSource instanceof Serializable, "dataSource must be Serializable");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
          .setDataSource(dataSource)
          .build();
    }

    public static DataSourceConfiguration create(String driverClassName, String url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return create(
          ValueProvider.StaticValueProvider.of(driverClassName),
          ValueProvider.StaticValueProvider.of(url));
    }

    public static DataSourceConfiguration create(
        ValueProvider<String> driverClassName, ValueProvider<String> url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
          .setDriverClassName(driverClassName)
          .setUrl(url)
          .build();
    }

    public DataSourceConfiguration withUsername(String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DataSourceConfiguration withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DataSourceConfiguration withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    /**
     * Sets the connection properties passed to driver.connect(...). Format of the string must be
     * [propertyName=property;]*
     *
     * <p>NOTE - The "user" and "password" properties can be add via {@link #withUsername(String)},
     * {@link #withPassword(String)}, so they do not need to be included here.
     */
    public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return withConnectionProperties(ValueProvider.StaticValueProvider.of(connectionProperties));
    }

    /** Same as {@link #withConnectionProperties(String)} but accepting a ValueProvider. */
    public DataSourceConfiguration withConnectionProperties(
        ValueProvider<String> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    /**
     * Sets the connection init sql statements to driver.connect(...).
     *
     * <p>NOTE - This property is not applicable across databases. Only MySQL and MariaDB support
     * this. A Sql exception is thrown if your database does not support it.
     */
    public DataSourceConfiguration withConnectionInitSqls(Collection<String> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      return withConnectionInitSqls(ValueProvider.StaticValueProvider.of(connectionInitSqls));
    }

    /** Same as {@link #withConnectionInitSqls(Collection)} but accepting a ValueProvider. */
    public DataSourceConfiguration withConnectionInitSqls(
        ValueProvider<Collection<String>> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      checkArgument(!connectionInitSqls.get().isEmpty(), "connectionInitSqls can not be empty");
      return builder().setConnectionInitSqls(connectionInitSqls).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", getDriverClassName()));
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
      }
    }

    DataSource buildDatasource() {
      if (getDataSource() == null) {
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
        if (getConnectionInitSqls() != null
            && getConnectionInitSqls().get() != null
            && !getConnectionInitSqls().get().isEmpty()) {
          basicDataSource.setConnectionInitSqls(getConnectionInitSqls().get());
        }

        return basicDataSource;
      }
      return getDataSource();
    }
  }

  /**
   * An interface used by the JdbcIO Write to set the parameters of the {@link PreparedStatement}
   * used to setParameters into the database.
   */
  @FunctionalInterface
  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }

  /** Implementation of {@link #readRows()}. */
  @AutoValue
  @Experimental(Kind.SCHEMAS)
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract @Nullable StatementPreparator getStatementPreparator();

    abstract int getFetchSize();

    abstract boolean getOutputParallelization();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder setQuery(ValueProvider<String> query);

      abstract Builder setStatementPreparator(StatementPreparator statementPreparator);

      abstract Builder setFetchSize(int fetchSize);

      abstract Builder setOutputParallelization(boolean outputParallelization);

      abstract ReadRows build();
    }

    public ReadRows withDataSourceConfiguration(DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public ReadRows withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public ReadRows withQuery(String query) {
      checkArgument(query != null, "query can not be null");
      return withQuery(ValueProvider.StaticValueProvider.of(query));
    }

    public ReadRows withQuery(ValueProvider<String> query) {
      checkArgument(query != null, "query can not be null");
      return toBuilder().setQuery(query).build();
    }

    public ReadRows withStatementPreparator(StatementPreparator statementPreparator) {
      checkArgument(statementPreparator != null, "statementPreparator can not be null");
      return toBuilder().setStatementPreparator(statementPreparator).build();
    }

    /**
     * This method is used to set the size of the data that is going to be fetched and loaded in
     * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
     * It should ONLY be used if the default value throws memory errors.
     */
    public ReadRows withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetch size must be > 0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public ReadRows withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      checkArgument(getQuery() != null, "withQuery() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      Schema schema = inferBeamSchema();
      PCollection<Row> rows =
          input.apply(
              JdbcIO.<Row>read()
                  .withDataSourceProviderFn(getDataSourceProviderFn())
                  .withQuery(getQuery())
                  .withCoder(RowCoder.of(schema))
                  .withRowMapper(SchemaUtil.BeamRowMapper.of(schema))
                  .withFetchSize(getFetchSize())
                  .withOutputParallelization(getOutputParallelization())
                  .withStatementPreparator(getStatementPreparator()));
      rows.setRowSchema(schema);
      return rows;
    }

    private Schema inferBeamSchema() {
      DataSource ds = getDataSourceProviderFn().apply(null);
      try (Connection conn = ds.getConnection();
          PreparedStatement statement =
              conn.prepareStatement(
                  getQuery().get(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        return SchemaUtil.toBeamSchema(statement.getMetaData());
      } catch (SQLException e) {
        throw new BeamSchemaInferenceException("Failed to infer Beam schema", e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract @Nullable StatementPreparator getStatementPreparator();

    abstract @Nullable RowMapper<T> getRowMapper();

    abstract @Nullable Coder<T> getCoder();

    abstract int getFetchSize();

    abstract boolean getOutputParallelization();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setStatementPreparator(StatementPreparator statementPreparator);

      abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setFetchSize(int fetchSize);

      abstract Builder<T> setOutputParallelization(boolean outputParallelization);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public Read<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
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

    /**
     * This method is used to set the size of the data that is going to be fetched and loaded in
     * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
     * It should ONLY be used if the default value throws memory errors.
     */
    public Read<T> withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetch size must be > 0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public Read<T> withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument(getQuery() != null, "withQuery() is required");
      checkArgument(getRowMapper() != null, "withRowMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input
          .apply(Create.of((Void) null))
          .apply(
              JdbcIO.<Void, T>readAll()
                  .withDataSourceProviderFn(getDataSourceProviderFn())
                  .withQuery(getQuery())
                  .withCoder(getCoder())
                  .withRowMapper(getRowMapper())
                  .withFetchSize(getFetchSize())
                  .withOutputParallelization(getOutputParallelization())
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
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract @Nullable ValueProvider<String> getQuery();

    abstract @Nullable PreparedStatementSetter<ParameterT> getParameterSetter();

    abstract @Nullable RowMapper<OutputT> getRowMapper();

    abstract @Nullable Coder<OutputT> getCoder();

    abstract int getFetchSize();

    abstract boolean getOutputParallelization();

    abstract Builder<ParameterT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<ParameterT, OutputT> setQuery(ValueProvider<String> query);

      abstract Builder<ParameterT, OutputT> setParameterSetter(
          PreparedStatementSetter<ParameterT> parameterSetter);

      abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

      abstract Builder<ParameterT, OutputT> setFetchSize(int fetchSize);

      abstract Builder<ParameterT, OutputT> setOutputParallelization(boolean outputParallelization);

      abstract ReadAll<ParameterT, OutputT> build();
    }

    public ReadAll<ParameterT, OutputT> withDataSourceConfiguration(
        DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public ReadAll<ParameterT, OutputT> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
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
      checkArgument(
          parameterSetter != null,
          "JdbcIO.readAll().withParameterSetter(parameterSetter) called "
              + "with null statementPreparator");
      return toBuilder().setParameterSetter(parameterSetter).build();
    }

    public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
      checkArgument(
          rowMapper != null,
          "JdbcIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(coder != null, "JdbcIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    /**
     * This method is used to set the size of the data that is going to be fetched and loaded in
     * memory per every database call. Please refer to: {@link java.sql.Statement#setFetchSize(int)}
     * It should ONLY be used if the default value throws memory errors.
     */
    public ReadAll<ParameterT, OutputT> withFetchSize(int fetchSize) {
      checkArgument(fetchSize > 0, "fetch size must be >0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public ReadAll<ParameterT, OutputT> withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      PCollection<OutputT> output =
          input
              .apply(
                  ParDo.of(
                      new ReadFn<>(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getParameterSetter(),
                          getRowMapper(),
                          getFetchSize())))
              .setCoder(getCoder());

      if (getOutputParallelization()) {
        output = output.apply(new Reparallelize<>());
      }

      try {
        TypeDescriptor<OutputT> typeDesc = getCoder().getEncodedTypeDescriptor();
        SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
        Schema schema = registry.getSchema(typeDesc);
        output.setSchema(
            schema,
            typeDesc,
            registry.getToRowFunction(typeDesc),
            registry.getFromRowFunction(typeDesc));
      } catch (NoSuchSchemaException e) {
        // ignore
      }

      return output;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** A {@link DoFn} executing the SQL query to read from the database. */
  private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> query;
    private final PreparedStatementSetter<ParameterT> parameterSetter;
    private final RowMapper<OutputT> rowMapper;
    private final int fetchSize;

    private DataSource dataSource;
    private Connection connection;

    private ReadFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> query,
        PreparedStatementSetter<ParameterT> parameterSetter,
        RowMapper<OutputT> rowMapper,
        int fetchSize) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.query = query;
      this.parameterSetter = parameterSetter;
      this.rowMapper = rowMapper;
      this.fetchSize = fetchSize;
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      // Only acquire the connection if we need to perform a read.
      if (connection == null) {
        connection = dataSource.getConnection();
      }
      try (PreparedStatement statement =
          connection.prepareStatement(
              query.get(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        statement.setFetchSize(fetchSize);
        parameterSetter.setParameters(context.element(), statement);
        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            context.output(rowMapper.mapRow(resultSet));
          }
        }
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      cleanUpConnection();
    }

    @Override
    protected void finalize() throws Throwable {
      cleanUpConnection();
    }

    private void cleanUpConnection() throws Exception {
      if (connection != null) {
        try {
          connection.close();
        } finally {
          connection = null;
        }
      }
    }
  }

  /**
   * Builder used to help with retry configuration for {@link JdbcIO}. The retry configuration
   * accepts maxAttempts and maxDuration for {@link FluentBackoff}.
   */
  @AutoValue
  public abstract static class RetryConfiguration implements Serializable {

    abstract int getMaxAttempts();

    abstract @Nullable Duration getMaxDuration();

    abstract @Nullable Duration getInitialDuration();

    abstract RetryConfiguration.Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMaxAttempts(int maxAttempts);

      abstract Builder setMaxDuration(Duration maxDuration);

      abstract Builder setInitialDuration(Duration initialDuration);

      abstract RetryConfiguration build();
    }

    public static RetryConfiguration create(
        int maxAttempts, @Nullable Duration maxDuration, @Nullable Duration initialDuration) {

      if (maxDuration == null || maxDuration.equals(Duration.ZERO)) {
        maxDuration = DEFAULT_MAX_CUMULATIVE_BACKOFF;
      }

      if (initialDuration == null || initialDuration.equals(Duration.ZERO)) {
        initialDuration = DEFAULT_INITIAL_BACKOFF;
      }

      checkArgument(maxAttempts > 0, "maxAttempts must be greater than 0");

      return new AutoValue_JdbcIO_RetryConfiguration.Builder()
          .setMaxAttempts(maxAttempts)
          .setInitialDuration(initialDuration)
          .setMaxDuration(maxDuration)
          .build();
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
   * An interface used to control if we retry the statements when a {@link SQLException} occurs. If
   * {@link RetryStrategy#apply(SQLException)} returns true, {@link Write} tries to replay the
   * statements.
   */
  @FunctionalInterface
  public interface RetryStrategy extends Serializable {
    boolean apply(SQLException sqlException);
  }

  /**
   * This class is used as the default return value of {@link JdbcIO#write()}.
   *
   * <p>All methods in this class delegate to the appropriate method of {@link JdbcIO.WriteVoid}.
   */
  public static class Write<T> extends PTransform<PCollection<T>, PDone> {
    WriteVoid<T> inner;

    Write() {
      this(JdbcIO.writeVoid());
    }

    Write(WriteVoid<T> inner) {
      this.inner = inner;
    }

    /** See {@link WriteVoid#withDataSourceConfiguration(DataSourceConfiguration)}. */
    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return new Write(inner.withDataSourceConfiguration(config));
    }

    /** See {@link WriteVoid#withDataSourceProviderFn(SerializableFunction)}. */
    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return new Write(inner.withDataSourceProviderFn(dataSourceProviderFn));
    }

    /** See {@link WriteVoid#withStatement(String)}. */
    public Write<T> withStatement(String statement) {
      return new Write(inner.withStatement(statement));
    }

    /** See {@link WriteVoid#withPreparedStatementSetter(PreparedStatementSetter)}. */
    public Write<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return new Write(inner.withPreparedStatementSetter(setter));
    }

    /** See {@link WriteVoid#withBatchSize(long)}. */
    public Write<T> withBatchSize(long batchSize) {
      return new Write(inner.withBatchSize(batchSize));
    }

    /** See {@link WriteVoid#withRetryStrategy(RetryStrategy)}. */
    public Write<T> withRetryStrategy(RetryStrategy retryStrategy) {
      return new Write(inner.withRetryStrategy(retryStrategy));
    }

    /** See {@link WriteVoid#withRetryConfiguration(RetryConfiguration)}. */
    public Write<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      return new Write(inner.withRetryConfiguration(retryConfiguration));
    }

    /** See {@link WriteVoid#withTable(String)}. */
    public Write<T> withTable(String table) {
      return new Write(inner.withTable(table));
    }

    /**
     * Returns {@link WriteVoid} transform which can be used in {@link Wait#on(PCollection[])} to
     * wait until all data is written.
     *
     * <p>Example: write a {@link PCollection} to one database and then to another database, making
     * sure that writing a window of data to the second database starts only after the respective
     * window has been fully written to the first database.
     *
     * <pre>{@code
     * PCollection<Void> firstWriteResults = data.apply(JdbcIO.write()
     *     .withDataSourceConfiguration(CONF_DB_1).withResults());
     * data.apply(Wait.on(firstWriteResults))
     *     .apply(JdbcIO.write().withDataSourceConfiguration(CONF_DB_2));
     * }</pre>
     */
    public WriteVoid<T> withResults() {
      return inner;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    private boolean hasStatementAndSetter() {
      return inner.getStatement() != null && inner.getPreparedStatementSetter() != null;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      // fixme: validate invalid table input
      if (input.hasSchema() && !hasStatementAndSetter()) {
        checkArgument(
            inner.getTable() != null, "table cannot be null if statement is not provided");
        Schema schema = input.getSchema();
        List<SchemaUtil.FieldWithIndex> fields = getFilteredFields(schema);
        inner =
            inner.withStatement(
                JdbcUtil.generateStatement(
                    inner.getTable(),
                    fields.stream()
                        .map(SchemaUtil.FieldWithIndex::getField)
                        .collect(Collectors.toList())));
        inner =
            inner.withPreparedStatementSetter(
                new AutoGeneratedPreparedStatementSetter(fields, input.getToRowFunction()));
      }

      inner.expand(input);
      return PDone.in(input.getPipeline());
    }

    private List<SchemaUtil.FieldWithIndex> getFilteredFields(Schema schema) {
      Schema tableSchema;

      try (Connection connection = inner.getDataSourceProviderFn().apply(null).getConnection();
          PreparedStatement statement =
              connection.prepareStatement((String.format("SELECT * FROM %s", inner.getTable())))) {
        tableSchema = SchemaUtil.toBeamSchema(statement.getMetaData());
        statement.close();
      } catch (SQLException e) {
        throw new RuntimeException(
            "Error while determining columns from table: " + inner.getTable(), e);
      }

      if (tableSchema.getFieldCount() < schema.getFieldCount()) {
        throw new RuntimeException("Input schema has more fields than actual table.");
      }

      // filter out missing fields from output table
      List<Schema.Field> missingFields =
          tableSchema.getFields().stream()
              .filter(
                  line ->
                      schema.getFields().stream()
                          .noneMatch(s -> s.getName().equalsIgnoreCase(line.getName())))
              .collect(Collectors.toList());

      // allow insert only if missing fields are nullable
      if (checkNullabilityForFields(missingFields)) {
        throw new RuntimeException("Non nullable fields are not allowed without schema.");
      }

      List<SchemaUtil.FieldWithIndex> tableFilteredFields =
          tableSchema.getFields().stream()
              .map(
                  (tableField) -> {
                    Optional<Schema.Field> optionalSchemaField =
                        schema.getFields().stream()
                            .filter((f) -> SchemaUtil.compareSchemaField(tableField, f))
                            .findFirst();
                    return (optionalSchemaField.isPresent())
                        ? SchemaUtil.FieldWithIndex.of(
                            tableField, schema.getFields().indexOf(optionalSchemaField.get()))
                        : null;
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());

      if (tableFilteredFields.size() != schema.getFieldCount()) {
        throw new RuntimeException("Provided schema doesn't match with database schema.");
      }

      return tableFilteredFields;
    }

    /**
     * A {@link org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter} implementation that
     * calls related setters on prepared statement.
     */
    private class AutoGeneratedPreparedStatementSetter implements PreparedStatementSetter<T> {

      private List<SchemaUtil.FieldWithIndex> fields;
      private SerializableFunction<T, Row> toRowFn;
      private List<PreparedStatementSetCaller> preparedStatementFieldSetterList = new ArrayList<>();

      AutoGeneratedPreparedStatementSetter(
          List<SchemaUtil.FieldWithIndex> fieldsWithIndex, SerializableFunction<T, Row> toRowFn) {
        this.fields = fieldsWithIndex;
        this.toRowFn = toRowFn;
        populatePreparedStatementFieldSetter();
      }

      private void populatePreparedStatementFieldSetter() {
        IntStream.range(0, fields.size())
            .forEach(
                (index) -> {
                  Schema.FieldType fieldType = fields.get(index).getField().getType();
                  preparedStatementFieldSetterList.add(
                      JdbcUtil.getPreparedStatementSetCaller(fieldType));
                });
      }

      @Override
      public void setParameters(T element, PreparedStatement preparedStatement) throws Exception {
        Row row = (element instanceof Row) ? (Row) element : toRowFn.apply(element);
        IntStream.range(0, fields.size())
            .forEach(
                (index) -> {
                  try {
                    preparedStatementFieldSetterList
                        .get(index)
                        .set(row, preparedStatement, index, fields.get(index));
                  } catch (SQLException | NullPointerException e) {
                    throw new RuntimeException("Error while setting data to preparedStatement", e);
                  }
                });
      }
    }
  }

  /** Interface implemented by functions that sets prepared statement data. */
  @FunctionalInterface
  interface PreparedStatementSetCaller extends Serializable {
    void set(
        Row element,
        PreparedStatement preparedStatement,
        int prepareStatementIndex,
        SchemaUtil.FieldWithIndex schemaFieldWithIndex)
        throws SQLException;
  }

  /** A {@link PTransform} to write to a JDBC datasource. */
  @AutoValue
  public abstract static class WriteVoid<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract @Nullable ValueProvider<String> getStatement();

    abstract long getBatchSize();

    abstract @Nullable PreparedStatementSetter<T> getPreparedStatementSetter();

    abstract @Nullable RetryStrategy getRetryStrategy();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable String getTable();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setStatement(ValueProvider<String> statement);

      abstract Builder<T> setBatchSize(long batchSize);

      abstract Builder<T> setPreparedStatementSetter(PreparedStatementSetter<T> setter);

      abstract Builder<T> setRetryStrategy(RetryStrategy deadlockPredicate);

      abstract Builder<T> setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder<T> setTable(String table);

      abstract WriteVoid<T> build();
    }

    public WriteVoid<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public WriteVoid<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public WriteVoid<T> withStatement(String statement) {
      return withStatement(ValueProvider.StaticValueProvider.of(statement));
    }

    public WriteVoid<T> withStatement(ValueProvider<String> statement) {
      return toBuilder().setStatement(statement).build();
    }

    public WriteVoid<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return toBuilder().setPreparedStatementSetter(setter).build();
    }

    /**
     * Provide a maximum size in number of SQL statement for the batch. Default is 1000.
     *
     * @param batchSize maximum batch size in number of statements
     */
    public WriteVoid<T> withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return toBuilder().setBatchSize(batchSize).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryStrategy} to determine if it
     * will retry the statements. If {@link RetryStrategy#apply(SQLException)} returns {@code true},
     * then {@link Write} retries the statements.
     */
    public WriteVoid<T> withRetryStrategy(RetryStrategy retryStrategy) {
      checkArgument(retryStrategy != null, "retryStrategy can not be null");
      return toBuilder().setRetryStrategy(retryStrategy).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryConfiguration} to
     * exponentially back off and retry the statements based on the {@link RetryConfiguration}
     * mentioned.
     *
     * <p>Usage of RetryConfiguration -
     *
     * <pre>{@code
     * pipeline.apply(JdbcIO.<T>write())
     *    .withDataSourceConfiguration(...)
     *    .withRetryStrategy(...)
     *    .withRetryConfiguration(JdbcIO.RetryConfiguration.
     *        create(5, Duration.standardSeconds(5), Duration.standardSeconds(1))
     *
     * }</pre>
     *
     * maxDuration and initialDuration are Nullable
     *
     * <pre>{@code
     * pipeline.apply(JdbcIO.<T>write())
     *    .withDataSourceConfiguration(...)
     *    .withRetryStrategy(...)
     *    .withRetryConfiguration(JdbcIO.RetryConfiguration.
     *        create(5, null, null)
     *
     * }</pre>
     */
    public WriteVoid<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration can not be null");
      return toBuilder().setRetryConfiguration(retryConfiguration).build();
    }

    public WriteVoid<T> withTable(String table) {
      checkArgument(table != null, "table name can not be null");
      return toBuilder().setTable(table).build();
    }

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      checkArgument(getStatement() != null, "withStatement() is required");
      checkArgument(
          getPreparedStatementSetter() != null, "withPreparedStatementSetter() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      return input.apply(ParDo.of(new WriteFn<>(this)));
    }

    private static class WriteFn<T> extends DoFn<T, Void> {

      private final WriteVoid<T> spec;
      private DataSource dataSource;
      private Connection connection;
      private PreparedStatement preparedStatement;
      private final List<T> records = new ArrayList<>();
      private static FluentBackoff retryBackOff;

      public WriteFn(WriteVoid<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        dataSource = spec.getDataSourceProviderFn().apply(null);
        RetryConfiguration retryConfiguration = spec.getRetryConfiguration();

        retryBackOff =
            FluentBackoff.DEFAULT
                .withInitialBackoff(retryConfiguration.getInitialDuration())
                .withMaxCumulativeBackoff(retryConfiguration.getMaxDuration())
                .withMaxRetries(retryConfiguration.getMaxAttempts());
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
        cleanUpStatementAndConnection();
      }

      @Override
      protected void finalize() throws Throwable {
        cleanUpStatementAndConnection();
      }

      private void cleanUpStatementAndConnection() throws Exception {
        try {
          if (preparedStatement != null) {
            try {
              preparedStatement.close();
            } finally {
              preparedStatement = null;
            }
          }
        } finally {
          if (connection != null) {
            try {
              connection.close();
            } finally {
              connection = null;
            }
          }
        }
      }

      private void executeBatch() throws SQLException, IOException, InterruptedException {
        if (records.isEmpty()) {
          return;
        }
        // Only acquire the connection if there is something to write.
        if (connection == null) {
          connection = dataSource.getConnection();
          connection.setAutoCommit(false);
          preparedStatement = connection.prepareStatement(spec.getStatement().get());
        }
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackOff.backoff();
        while (true) {
          try (PreparedStatement preparedStatement =
              connection.prepareStatement(spec.getStatement().get())) {
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

  /**
   * Wraps a {@link DataSourceConfiguration} to provide a {@link PoolingDataSource}.
   *
   * <p>At most a single {@link DataSource} instance will be constructed during pipeline execution
   * for each unique {@link DataSourceConfiguration} within the pipeline.
   */
  public static class PoolableDataSourceProvider
      implements SerializableFunction<Void, DataSource>, HasDisplayData {
    private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
        new ConcurrentHashMap<>();
    private final DataSourceProviderFromDataSourceConfiguration config;

    private PoolableDataSourceProvider(DataSourceConfiguration config) {
      this.config = new DataSourceProviderFromDataSourceConfiguration(config);
    }

    public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
      return new PoolableDataSourceProvider(config);
    }

    @Override
    public DataSource apply(Void input) {
      return instances.computeIfAbsent(
          config.config,
          ignored -> {
            DataSource basicSource = config.apply(input);
            DataSourceConnectionFactory connectionFactory =
                new DataSourceConnectionFactory(basicSource);
            PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setMinIdle(0);
            poolConfig.setMinEvictableIdleTimeMillis(10000);
            poolConfig.setSoftMinEvictableIdleTimeMillis(30000);
            GenericObjectPool connectionPool =
                new GenericObjectPool(poolableConnectionFactory, poolConfig);
            poolableConnectionFactory.setPool(connectionPool);
            poolableConnectionFactory.setDefaultAutoCommit(false);
            poolableConnectionFactory.setDefaultReadOnly(false);
            return new PoolingDataSource(connectionPool);
          });
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }

  /**
   * Wraps a {@link DataSourceConfiguration} to provide a {@link DataSource}.
   *
   * <p>At most a single {@link DataSource} instance will be constructed during pipeline execution
   * for each unique {@link DataSourceConfiguration} within the pipeline.
   */
  public static class DataSourceProviderFromDataSourceConfiguration
      implements SerializableFunction<Void, DataSource>, HasDisplayData {
    private static final ConcurrentHashMap<DataSourceConfiguration, DataSource> instances =
        new ConcurrentHashMap<>();
    private final DataSourceConfiguration config;

    private DataSourceProviderFromDataSourceConfiguration(DataSourceConfiguration config) {
      this.config = config;
    }

    public static SerializableFunction<Void, DataSource> of(DataSourceConfiguration config) {
      return new DataSourceProviderFromDataSourceConfiguration(config);
    }

    @Override
    public DataSource apply(Void input) {
      return instances.computeIfAbsent(config, DataSourceConfiguration::buildDatasource);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }
}
