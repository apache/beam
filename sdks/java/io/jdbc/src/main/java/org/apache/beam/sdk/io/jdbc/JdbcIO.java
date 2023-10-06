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
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.Serializable;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO.WriteFn.WriteFnSpec;
import org.apache.beam.sdk.io.jdbc.JdbcUtil.PartitioningFn;
import org.apache.beam.sdk.io.jdbc.SchemaUtil.FieldWithIndex;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DataSourceConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
 * <pre><code>
 * private static class MyDataSourceProviderFn implements{@literal SerializableFunction<Void, DataSource>} {
 *   private static transient DataSource dataSource;
 *
 *  {@literal @Override}
 *   public synchronized DataSource apply(Void input) {
 *     if (dataSource == null) {
 *       dataSource = ... build data source ...
 *     }
 *     return dataSource;
 *   }
 * }
 * {@literal
 * pipeline.apply(JdbcIO.<KV<Integer, String>>read()
 *   .withDataSourceProviderFn(new MyDataSourceProviderFn())
 *   // ...
 * );
 * }</code></pre>
 *
 * <h4>Parallel reading from a JDBC datasource</h4>
 *
 * <p>Beam supports partitioned reading of all data from a table. Automatic partitioning is
 * supported for a few data types: {@link Long}, {@link org.joda.time.DateTime}, {@link String}. To
 * enable this, use {@link JdbcIO#readWithPartitions(TypeDescriptor)}.
 *
 * <p>The partitioning scheme depends on these parameters, which can be user-provided, or
 * automatically inferred by Beam (for the supported types):
 *
 * <ul>
 *   <li>Upper bound
 *   <li>Lower bound
 *   <li>Number of partitions - when auto-inferred, the number of partitions defaults to the square
 *       root of the number of rows divided by 5 (i.e.: {@code Math.floor(Math.sqrt(numRows) / 5)}).
 * </ul>
 *
 * <p>To trigger auto-inference of these parameters, the user just needs to not provide them. To
 * infer them automatically, Beam runs either of these statements:
 *
 * <ul>
 *   <li>{@code SELECT min(column), max(column), COUNT(*) from table} when none of the parameters is
 *       passed to the transform.
 *   <li>{@code SELECT min(column), max(column) from table} when only number of partitions is
 *       provided, but not upper or lower bounds.
 * </ul>
 *
 * <p><b>Should I use this transform?</b> Consider using this transform in the following situations:
 *
 * <ul>
 *   <li>The partitioning column is indexed. This will help speed up the range queries
 *   <li><b>Use auto-inference</b> if the queries for bound and partition inference are efficient to
 *       execute in your DBMS.
 *   <li>The distribution of data over the partitioning column is <i>roughly uniform</i>. Uniformity
 *       is not mandatory, but this transform will work best in that situation.
 * </ul>
 *
 * <p>The following example shows usage of <b>auto-inferred ranges, number of partitions, and
 * schema</b>
 *
 * <pre>{@code
 * pipeline.apply(JdbcIO.<Row>readWithPartitions()
 *  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *         "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
 *       .withUsername("username")
 *       .withPassword("password"))
 *  .withTable("Person")
 *  .withPartitionColumn("id")
 *  .withRowOutput()
 * );
 * }</pre>
 *
 * <p>Instead of a full table you could also use a subquery in parentheses. The subquery can be
 * specified using Table option instead and partition columns can be qualified using the subquery
 * alias provided as part of Table. <b>Note</b> that a subquery may not perform as well with
 * auto-inferred ranges and partitions, because it may not rely on indices to speed up the
 * partitioning.
 *
 * <pre>{@code
 * pipeline.apply(JdbcIO.<KV<Integer, String>>readWithPartitions()
 *  .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
 *         "com.mysql.jdbc.Driver", "jdbc:mysql://hostname:3306/mydb")
 *       .withUsername("username")
 *       .withPassword("password"))
 *  .withTable("(select id, name from Person) as subq")
 *  .withPartitionColumn("id")
 *  .withLowerBound(0)
 *  .withUpperBound(1000)
 *  .withNumPartitions(5)
 *  .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
 *    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *      return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *    }
 *  })
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
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
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

  /**
   * Like {@link #readAll}, but executes multiple instances of the query on the same table
   * (subquery) using ranges.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T, PartitionColumnT> ReadWithPartitions<T, PartitionColumnT> readWithPartitions(
      TypeDescriptor<PartitionColumnT> partitioningColumnType) {
    return new AutoValue_JdbcIO_ReadWithPartitions.Builder<T, PartitionColumnT>()
        .setPartitionColumnType(partitioningColumnType)
        .setNumPartitions(DEFAULT_NUM_PARTITIONS)
        .setUseBeamSchema(false)
        .build();
  }

  public static <T> ReadWithPartitions<T, Long> readWithPartitions() {
    return JdbcIO.<T, Long>readWithPartitions(TypeDescriptors.longs());
  }

  private static final long DEFAULT_BATCH_SIZE = 1000L;
  private static final int DEFAULT_FETCH_SIZE = 50_000;
  // Default values used from fluent backoff.
  private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.standardSeconds(1);
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardDays(1000);
  // Default value used for partitioning a table
  private static final int DEFAULT_NUM_PARTITIONS = 200;

  /**
   * Write data to a JDBC datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new Write<>();
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
   * {@link SQLException#getSQLState()} equals 40001 or 40P01. 40001 is the SQL State used by most
   * of databases to identify deadlock, and 40P01 is specific to PostgreSQL (see <a
   * href="https://www.postgresql.org/docs/13/errcodes-appendix.html">PostgreSQL documentation</a>).
   */
  public static class DefaultRetryStrategy implements RetryStrategy {
    private static final Set<String> errorCodesToRetry =
        new HashSet(Arrays.asList("40001", "40P01"));

    @Override
    public boolean apply(SQLException e) {
      String sqlState = e.getSQLState();
      return sqlState != null && errorCodesToRetry.contains(sqlState);
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

    @Pure
    abstract @Nullable ValueProvider<String> getDriverClassName();

    @Pure
    abstract @Nullable ValueProvider<String> getUrl();

    @Pure
    abstract @Nullable ValueProvider<@Nullable String> getUsername();

    @Pure
    abstract @Nullable ValueProvider<@Nullable String> getPassword();

    @Pure
    abstract @Nullable ValueProvider<String> getConnectionProperties();

    @Pure
    abstract @Nullable ValueProvider<Collection<String>> getConnectionInitSqls();

    @Pure
    abstract @Nullable ValueProvider<Integer> getMaxConnections();

    @Pure
    abstract @Nullable ClassLoader getDriverClassLoader();

    @Pure
    abstract @Nullable ValueProvider<String> getDriverJars();

    @Pure
    abstract @Nullable DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDriverClassName(ValueProvider<@Nullable String> driverClassName);

      abstract Builder setUrl(ValueProvider<@Nullable String> url);

      abstract Builder setUsername(ValueProvider<@Nullable String> username);

      abstract Builder setPassword(ValueProvider<@Nullable String> password);

      abstract Builder setConnectionProperties(
          ValueProvider<@Nullable String> connectionProperties);

      abstract Builder setConnectionInitSqls(
          ValueProvider<Collection<@Nullable String>> connectionInitSqls);

      abstract Builder setMaxConnections(ValueProvider<@Nullable Integer> maxConnections);

      abstract Builder setDriverClassLoader(ClassLoader driverClassLoader);

      abstract Builder setDriverJars(ValueProvider<String> driverJars);

      abstract Builder setDataSource(@Nullable DataSource dataSource);

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
        ValueProvider<@Nullable String> driverClassName, ValueProvider<@Nullable String> url) {
      checkArgument(driverClassName != null, "driverClassName can not be null");
      checkArgument(url != null, "url can not be null");
      return new AutoValue_JdbcIO_DataSourceConfiguration.Builder()
          .setDriverClassName(driverClassName)
          .setUrl(url)
          .build();
    }

    public DataSourceConfiguration withUsername(@Nullable String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DataSourceConfiguration withUsername(ValueProvider<@Nullable String> username) {
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(@Nullable String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DataSourceConfiguration withPassword(ValueProvider<@Nullable String> password) {
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
        ValueProvider<@Nullable String> connectionProperties) {
      checkArgument(connectionProperties != null, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    /**
     * Sets the connection init sql statements to driver.connect(...).
     *
     * <p>NOTE - This property is not applicable across databases. Only MySQL and MariaDB support
     * this. A Sql exception is thrown if your database does not support it.
     */
    public DataSourceConfiguration withConnectionInitSqls(
        Collection<@Nullable String> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      return withConnectionInitSqls(ValueProvider.StaticValueProvider.of(connectionInitSqls));
    }

    /** Same as {@link #withConnectionInitSqls(Collection)} but accepting a ValueProvider. */
    public DataSourceConfiguration withConnectionInitSqls(
        ValueProvider<Collection<@Nullable String>> connectionInitSqls) {
      checkArgument(connectionInitSqls != null, "connectionInitSqls can not be null");
      checkArgument(!connectionInitSqls.get().isEmpty(), "connectionInitSqls can not be empty");
      return builder().setConnectionInitSqls(connectionInitSqls).build();
    }

    /** Sets the maximum total number of connections. Use a negative value for no limit. */
    public DataSourceConfiguration withMaxConnections(Integer maxConnections) {
      checkArgument(maxConnections != null, "maxConnections can not be null");
      return withMaxConnections(ValueProvider.StaticValueProvider.of(maxConnections));
    }

    /** Same as {@link #withMaxConnections(Integer)} but accepting a ValueProvider. */
    public DataSourceConfiguration withMaxConnections(
        ValueProvider<@Nullable Integer> maxConnections) {
      return builder().setMaxConnections(maxConnections).build();
    }

    /**
     * Sets the class loader instance to be used to load the JDBC driver. If not specified, the
     * default class loader is used.
     */
    public DataSourceConfiguration withDriverClassLoader(ClassLoader driverClassLoader) {
      checkArgument(driverClassLoader != null, "driverClassLoader can not be null");
      return builder().setDriverClassLoader(driverClassLoader).build();
    }

    /**
     * Comma separated paths for JDBC drivers. This method is filesystem agnostic and can be used
     * for all FileSystems supported by Beam If not specified, the default classloader is used to
     * load the jars.
     *
     * <p>For example, gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar.
     */
    public DataSourceConfiguration withDriverJars(String driverJars) {
      checkArgument(driverJars != null, "driverJars can not be null");
      return withDriverJars(ValueProvider.StaticValueProvider.of(driverJars));
    }

    /** Same as {@link #withDriverJars(String)} but accepting a ValueProvider. */
    public DataSourceConfiguration withDriverJars(ValueProvider<String> driverJars) {
      checkArgument(driverJars != null, "driverJars can not be null");
      return builder().setDriverJars(driverJars).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcDriverClassName", getDriverClassName()));
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
        builder.addIfNotNull(DisplayData.item("driverJars", getDriverJars()));
      }
    }

    public DataSource buildDatasource() {
      if (getDataSource() == null) {
        BasicDataSource basicDataSource = new BasicDataSource();
        if (getDriverClassName() != null) {
          basicDataSource.setDriverClassName(getDriverClassName().get());
        }
        if (getUrl() != null) {
          basicDataSource.setUrl(getUrl().get());
        }
        if (getUsername() != null) {
          @SuppressWarnings(
              "nullness") // this is actually nullable, but apache commons dbcp2 not annotated
          @NonNull
          String username = getUsername().get();
          basicDataSource.setUsername(username);
        }
        if (getPassword() != null) {
          @SuppressWarnings(
              "nullness") // this is actually nullable, but apache commons dbcp2 not annotated
          @NonNull
          String password = getPassword().get();
          basicDataSource.setPassword(password);
        }
        if (getConnectionProperties() != null && getConnectionProperties().get() != null) {
          basicDataSource.setConnectionProperties(getConnectionProperties().get());
        }
        if (getConnectionInitSqls() != null
            && getConnectionInitSqls().get() != null
            && !getConnectionInitSqls().get().isEmpty()) {
          basicDataSource.setConnectionInitSqls(getConnectionInitSqls().get());
        }
        if (getMaxConnections() != null && getMaxConnections().get() != null) {
          basicDataSource.setMaxTotal(getMaxConnections().get());
        }
        if (getDriverClassLoader() != null) {
          basicDataSource.setDriverClassLoader(getDriverClassLoader());
        }
        if (getDriverJars() != null) {
          URLClassLoader classLoader =
              URLClassLoader.newInstance(JdbcUtil.saveFilesLocally(getDriverJars().get()));
          basicDataSource.setDriverClassLoader(classLoader);
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
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    @Pure
    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Pure
    abstract @Nullable ValueProvider<String> getQuery();

    @Pure
    abstract @Nullable StatementPreparator getStatementPreparator();

    @Pure
    abstract int getFetchSize();

    @Pure
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
      // Note that api.java.sql.Statement#setFetchSize says it only accepts values >= 0
      // and that MySQL supports using Integer.MIN_VALUE as a hint to stream the ResultSet instead
      // of loading it into memory. See
      // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html for additional details.
      checkArgument(
          fetchSize >= 0 || fetchSize == Integer.MIN_VALUE,
          "fetch size must be >= 0 or equal to Integer.MIN_VALUE");
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
      ValueProvider<String> query = checkStateNotNull(getQuery(), "withQuery() is required");
      SerializableFunction<Void, DataSource> dataSourceProviderFn =
          checkStateNotNull(
              getDataSourceProviderFn(),
              "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      Schema schema = inferBeamSchema(dataSourceProviderFn.apply(null), query.get());
      PCollection<Row> rows =
          input.apply(
              JdbcIO.<Row>read()
                  .withDataSourceProviderFn(dataSourceProviderFn)
                  .withQuery(query)
                  .withCoder(RowCoder.of(schema))
                  .withRowMapper(SchemaUtil.BeamRowMapper.of(schema))
                  .withFetchSize(getFetchSize())
                  .withOutputParallelization(getOutputParallelization())
                  .withStatementPreparator(checkStateNotNull(getStatementPreparator())));
      rows.setRowSchema(schema);
      return rows;
    }

    // Spotbugs seems to not understand the multi-statement try-with-resources
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    public static Schema inferBeamSchema(DataSource ds, String query) {
      try (Connection conn = ds.getConnection();
          PreparedStatement statement =
              conn.prepareStatement(
                  query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
        ResultSetMetaData metadata =
            checkStateNotNull(statement.getMetaData(), "could not get statement metadata");
        return SchemaUtil.toBeamSchema(metadata);
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

    @Pure
    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Pure
    abstract @Nullable ValueProvider<String> getQuery();

    @Pure
    abstract @Nullable StatementPreparator getStatementPreparator();

    @Pure
    abstract @Nullable RowMapper<T> getRowMapper();

    @Pure
    abstract @Nullable Coder<T> getCoder();

    @Pure
    abstract int getFetchSize();

    @Pure
    abstract boolean getOutputParallelization();

    @Pure
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
      checkArgumentNotNull(statementPreparator, "statementPreparator can not be null");
      return toBuilder().setStatementPreparator(statementPreparator).build();
    }

    public Read<T> withRowMapper(RowMapper<T> rowMapper) {
      checkArgumentNotNull(rowMapper, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    /**
     * @deprecated
     *     <p>{@link JdbcIO} is able to infer appropriate coders from other parameters.
     */
    @Deprecated
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
      // Note that api.java.sql.Statement#setFetchSize says it only accepts values >= 0
      // and that MySQL supports using Integer.MIN_VALUE as a hint to stream the ResultSet instead
      // of loading it into memory. See
      // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html for additional details.
      checkArgument(
          fetchSize >= 0 || fetchSize == Integer.MIN_VALUE,
          "fetch size must be >= 0 or equal to Integer.MIN_VALUE");
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
      ValueProvider<String> query = checkArgumentNotNull(getQuery(), "withQuery() is required");
      RowMapper<T> rowMapper = checkArgumentNotNull(getRowMapper(), "withRowMapper() is required");
      SerializableFunction<Void, DataSource> dataSourceProviderFn =
          checkArgumentNotNull(
              getDataSourceProviderFn(),
              "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      JdbcIO.ReadAll<Void, T> readAll =
          JdbcIO.<Void, T>readAll()
              .withDataSourceProviderFn(dataSourceProviderFn)
              .withQuery(query)
              .withRowMapper(rowMapper)
              .withFetchSize(getFetchSize())
              .withOutputParallelization(getOutputParallelization())
              .withParameterSetter(
                  (element, preparedStatement) -> {
                    if (getStatementPreparator() != null) {
                      getStatementPreparator().setParameters(preparedStatement);
                    }
                  });

      @Nullable Coder<T> coder = getCoder();
      if (coder != null) {
        readAll = readAll.toBuilder().setCoder(coder).build();
      }
      return input.apply(Create.of((Void) null)).apply(readAll);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));

      if (getRowMapper() != null) {
        builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      }
      if (getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      }
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    @Pure
    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Pure
    abstract @Nullable ValueProvider<String> getQuery();

    @Pure
    abstract @Nullable PreparedStatementSetter<ParameterT> getParameterSetter();

    @Pure
    abstract @Nullable RowMapper<OutputT> getRowMapper();

    @Pure
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
      if (getDataSourceProviderFn() != null) {
        throw new IllegalArgumentException(
            "A dataSourceConfiguration or dataSourceProviderFn has "
                + "already been provided, and does not need to be provided again.");
      }
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
      checkArgumentNotNull(
          parameterSetter,
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

    /**
     * @deprecated
     *     <p>{@link JdbcIO} is able to infer appropriate coders from other parameters.
     */
    @Deprecated
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
      // Note that api.java.sql.Statement#setFetchSize says it only accepts values >= 0
      // and that MySQL supports using Integer.MIN_VALUE as a hint to stream the ResultSet instead
      // of loading it into memory. See
      // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html for additional details.
      checkArgument(
          fetchSize >= 0 || fetchSize == Integer.MIN_VALUE,
          "fetch size must be >= 0 or equal to Integer.MIN_VALUE");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public ReadAll<ParameterT, OutputT> withOutputParallelization(boolean outputParallelization) {
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    private @Nullable Coder<OutputT> inferCoder(
        CoderRegistry registry, SchemaRegistry schemaRegistry) {
      if (getCoder() != null) {
        return getCoder();
      } else {
        RowMapper<OutputT> rowMapper = getRowMapper();
        TypeDescriptor<OutputT> outputType =
            TypeDescriptors.extractFromTypeParameters(
                rowMapper,
                RowMapper.class,
                new TypeVariableExtractor<RowMapper<OutputT>, OutputT>() {});
        try {
          return schemaRegistry.getSchemaCoder(outputType);
        } catch (NoSuchSchemaException e) {
          LOG.warn(
              "Unable to infer a schema for type {}. Attempting to infer a coder without a schema.",
              outputType);
        }
        try {
          return registry.getCoder(outputType);
        } catch (CannotProvideCoderException e) {
          LOG.warn("Unable to infer a coder for type {}", outputType);
          return null;
        }
      }
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {
      Coder<OutputT> coder =
          inferCoder(
              input.getPipeline().getCoderRegistry(), input.getPipeline().getSchemaRegistry());
      checkStateNotNull(
          coder,
          "Unable to infer a coder for JdbcIO.readAll() transform. "
              + "Provide a coder via withCoder, or ensure that one can be inferred from the"
              + " provided RowMapper.");
      PCollection<OutputT> output =
          input
              .apply(
                  ParDo.of(
                      new ReadFn<>(
                          checkStateNotNull(getDataSourceProviderFn()),
                          checkStateNotNull(getQuery()),
                          checkStateNotNull(getParameterSetter()),
                          checkStateNotNull(getRowMapper()),
                          getFetchSize())))
              .setCoder(coder);

      if (getOutputParallelization()) {
        output = output.apply(new Reparallelize<>());
      }

      try {
        TypeDescriptor<OutputT> typeDesc = coder.getEncodedTypeDescriptor();
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

      if (getRowMapper() != null) {
        builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      }
      if (getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      }
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #readWithPartitions}. */
  @AutoValue
  public abstract static class ReadWithPartitions<T, PartitionColumnT>
      extends PTransform<PBegin, PCollection<T>> {

    @Pure
    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Pure
    abstract @Nullable RowMapper<T> getRowMapper();

    @Pure
    abstract @Nullable Coder<T> getCoder();

    @Pure
    abstract @Nullable Integer getNumPartitions();

    @Pure
    abstract @Nullable String getPartitionColumn();

    @Pure
    abstract boolean getUseBeamSchema();

    @Pure
    abstract @Nullable PartitionColumnT getLowerBound();

    @Pure
    abstract @Nullable PartitionColumnT getUpperBound();

    @Pure
    abstract @Nullable String getTable();

    @Pure
    abstract TypeDescriptor<PartitionColumnT> getPartitionColumnType();

    @Pure
    abstract Builder<T, PartitionColumnT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T, PartitionColumnT> {

      abstract Builder<T, PartitionColumnT> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T, PartitionColumnT> setRowMapper(RowMapper<T> rowMapper);

      abstract Builder<T, PartitionColumnT> setCoder(Coder<T> coder);

      abstract Builder<T, PartitionColumnT> setNumPartitions(int numPartitions);

      abstract Builder<T, PartitionColumnT> setPartitionColumn(String partitionColumn);

      abstract Builder<T, PartitionColumnT> setLowerBound(PartitionColumnT lowerBound);

      abstract Builder<T, PartitionColumnT> setUpperBound(PartitionColumnT upperBound);

      abstract Builder<T, PartitionColumnT> setUseBeamSchema(boolean useBeamSchema);

      abstract Builder<T, PartitionColumnT> setTable(String tableName);

      abstract Builder<T, PartitionColumnT> setPartitionColumnType(
          TypeDescriptor<PartitionColumnT> partitionColumnType);

      abstract ReadWithPartitions<T, PartitionColumnT> build();
    }

    public ReadWithPartitions<T, PartitionColumnT> withDataSourceConfiguration(
        final DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public ReadWithPartitions<T, PartitionColumnT> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public ReadWithPartitions<T, PartitionColumnT> withRowMapper(RowMapper<T> rowMapper) {
      checkNotNull(rowMapper, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    /**
     * @deprecated
     *     <p>{@link JdbcIO} is able to infer appropriate coders from other parameters.
     */
    @Deprecated
    public ReadWithPartitions<T, PartitionColumnT> withCoder(Coder<T> coder) {
      checkNotNull(coder, "coder can not be null");
      return toBuilder().setCoder(coder).build();
    }

    /**
     * The number of partitions. This, along with withLowerBound and withUpperBound, form partitions
     * strides for generated WHERE clause expressions used to split the column withPartitionColumn
     * evenly. When the input is less than 1, the number is set to 1.
     */
    public ReadWithPartitions<T, PartitionColumnT> withNumPartitions(int numPartitions) {
      checkArgument(numPartitions > 0, "numPartitions can not be less than 1");
      return toBuilder().setNumPartitions(numPartitions).build();
    }

    /** The name of a column of numeric type that will be used for partitioning. */
    public ReadWithPartitions<T, PartitionColumnT> withPartitionColumn(String partitionColumn) {
      checkNotNull(partitionColumn, "partitionColumn can not be null");
      return toBuilder().setPartitionColumn(partitionColumn).build();
    }

    /** Data output type is {@link Row}, and schema is auto-inferred from the database. */
    public ReadWithPartitions<T, PartitionColumnT> withRowOutput() {
      return toBuilder().setUseBeamSchema(true).build();
    }

    public ReadWithPartitions<T, PartitionColumnT> withLowerBound(PartitionColumnT lowerBound) {
      return toBuilder().setLowerBound(lowerBound).build();
    }

    public ReadWithPartitions<T, PartitionColumnT> withUpperBound(PartitionColumnT upperBound) {
      return toBuilder().setUpperBound(upperBound).build();
    }

    /** Name of the table in the external database. Can be used to pass a user-defined subqery. */
    public ReadWithPartitions<T, PartitionColumnT> withTable(String tableName) {
      checkNotNull(tableName, "table can not be null");
      return toBuilder().setTable(tableName).build();
    }

    private static final int EQUAL = 0;

    @Override
    public PCollection<T> expand(PBegin input) {
      SerializableFunction<Void, DataSource> dataSourceProviderFn =
          checkStateNotNull(
              getDataSourceProviderFn(),
              "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
      String partitionColumn =
          checkStateNotNull(getPartitionColumn(), "withPartitionColumn() is required");
      String table = checkStateNotNull(getTable(), "withTable() is required");
      checkArgument(
          // We XOR so that only one of these is true / provided. (^ is an xor operator : ))
          getUseBeamSchema() ^ getRowMapper() != null,
          "Provide only withRowOutput() or withRowMapper() arguments for "
              + "JdbcIO.readWithPartitions). These are mutually exclusive.");
      checkArgument(
          (getUpperBound() != null) == (getLowerBound() != null),
          "When providing either lower or upper bound, both "
              + "parameters are mandatory for JdbcIO.readWithPartitions");
      if (getLowerBound() != null
          && getUpperBound() != null
          && getLowerBound() instanceof Comparable<?>) {
        // Not all partition types are comparable. For example, LocalDateTime, which is a valid
        // partitioning type, is not Comparable, so we can't enforce this for all sorts of
        // partitioning.
        checkArgument(
            ((Comparable<PartitionColumnT>) getLowerBound()).compareTo(getUpperBound()) < EQUAL,
            "The lower bound of partitioning column is larger or equal than the upper bound");
      }
      checkNotNull(
          JdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(getPartitionColumnType()),
          "readWithPartitions only supports the following types: %s",
          JdbcUtil.PRESET_HELPERS.keySet());

      PCollection<KV<Long, KV<PartitionColumnT, PartitionColumnT>>> params;

      if (getLowerBound() == null && getUpperBound() == null) {
        String query =
            String.format(
                "SELECT min(%s), max(%s) FROM %s", partitionColumn, partitionColumn, table);
        if (getNumPartitions() == null) {
          query =
              String.format(
                  "SELECT min(%s), max(%s), count(*) FROM %s",
                  partitionColumn, partitionColumn, table);
        }
        params =
            input
                .apply(
                    JdbcIO.<KV<Long, KV<PartitionColumnT, PartitionColumnT>>>read()
                        .withQuery(query)
                        .withDataSourceProviderFn(dataSourceProviderFn)
                        .withRowMapper(
                            checkStateNotNull(
                                JdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(
                                    getPartitionColumnType()))))
                .apply(
                    MapElements.via(
                        new SimpleFunction<
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>>,
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>>>() {
                          @Override
                          public KV<Long, KV<PartitionColumnT, PartitionColumnT>> apply(
                              KV<Long, KV<PartitionColumnT, PartitionColumnT>> input) {
                            KV<Long, KV<PartitionColumnT, PartitionColumnT>> result;
                            if (getNumPartitions() == null) {
                              // In this case, we use the table row count to infer a number of
                              // partitions.
                              // We take the square root of the number of rows, and divide it by 10
                              // to keep a relatively low number of partitions, given that an RDBMS
                              // cannot usually accept a very large number of connections.
                              long numPartitions =
                                  Math.max(
                                      1, Math.round(Math.floor(Math.sqrt(input.getKey()) / 10)));
                              result = KV.of(numPartitions, input.getValue());
                            } else {
                              result = KV.of(getNumPartitions().longValue(), input.getValue());
                            }
                            LOG.info(
                                "Inferred min: {} - max: {} - numPartitions: {}",
                                result.getValue().getKey(),
                                result.getValue().getValue(),
                                result.getKey());
                            return result;
                          }
                        }));
      } else {
        params =
            input.apply(
                Create.of(
                    KV.of(
                        checkStateNotNull(getNumPartitions()).longValue(),
                        KV.of(getLowerBound(), getUpperBound()))));
      }

      RowMapper<T> rowMapper = null;
      Schema schema = null;
      if (getUseBeamSchema()) {
        schema =
            ReadRows.inferBeamSchema(
                dataSourceProviderFn.apply(null), String.format("SELECT * FROM %s", getTable()));
        rowMapper = (RowMapper<T>) SchemaUtil.BeamRowMapper.of(schema);
      } else {
        rowMapper = getRowMapper();
      }
      checkStateNotNull(rowMapper);

      PCollection<KV<PartitionColumnT, PartitionColumnT>> ranges =
          params
              .apply("Partitioning", ParDo.of(new PartitioningFn<>(getPartitionColumnType())))
              .apply("Reshuffle partitions", Reshuffle.viaRandomKey());

      JdbcIO.ReadAll<KV<PartitionColumnT, PartitionColumnT>, T> readAll =
          JdbcIO.<KV<PartitionColumnT, PartitionColumnT>, T>readAll()
              .withDataSourceProviderFn(dataSourceProviderFn)
              .withQuery(
                  String.format(
                      "select * from %1$s where %2$s >= ? and %2$s < ?", table, partitionColumn))
              .withRowMapper(rowMapper)
              .withParameterSetter(
                  checkStateNotNull(
                          JdbcUtil.JdbcReadWithPartitionsHelper.getPartitionsHelper(
                              getPartitionColumnType()))
                      ::setParameters)
              .withOutputParallelization(false);

      if (getUseBeamSchema()) {
        checkStateNotNull(schema);
        readAll = readAll.withCoder((Coder<T>) RowCoder.of(schema));
      } else if (getCoder() != null) {
        readAll = readAll.withCoder(getCoder());
      }

      return ranges.apply("Read ranges", readAll);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item(
              "rowMapper",
              getRowMapper() == null
                  ? "auto-infer"
                  : getRowMapper().getClass().getCanonicalName()));
      if (getCoder() != null) {
        builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      }
      builder.add(DisplayData.item("partitionColumn", getPartitionColumn()));
      builder.add(DisplayData.item("table", getTable()));
      builder.add(
          DisplayData.item(
              "numPartitions",
              getNumPartitions() == null ? "auto-infer" : getNumPartitions().toString()));
      builder.add(
          DisplayData.item(
              "lowerBound", getLowerBound() == null ? "auto-infer" : getLowerBound().toString()));
      builder.add(
          DisplayData.item(
              "upperBound", getUpperBound() == null ? "auto-infer" : getUpperBound().toString()));
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

    private @Nullable DataSource dataSource;
    private @Nullable Connection connection;

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

    private Connection getConnection() throws SQLException {
      if (this.connection == null) {
        this.connection = checkStateNotNull(this.dataSource).getConnection();
      }
      return this.connection;
    }

    @ProcessElement
    // Spotbugs seems to not understand the nested try-with-resources
    @SuppressFBWarnings({
      "OBL_UNSATISFIED_OBLIGATION",
      "ODR_OPEN_DATABASE_RESOURCE", // connection closed in finishbundle
    })
    public void processElement(ProcessContext context) throws Exception {
      // Only acquire the connection if we need to perform a read.
      Connection connection = getConnection();
      // PostgreSQL requires autocommit to be disabled to enable cursor streaming
      // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
      LOG.info("Autocommit has been disabled");
      connection.setAutoCommit(false);
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

    @Teardown
    public void tearDown() throws Exception {
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

    /** See {@link WriteVoid#withAutoSharding()}. */
    public Write<T> withAutoSharding() {
      return new Write<>(inner.withAutoSharding());
    }

    /** See {@link WriteVoid#withDataSourceConfiguration(DataSourceConfiguration)}. */
    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      return new Write<>(inner.withDataSourceConfiguration(config));
    }

    /** See {@link WriteVoid#withDataSourceProviderFn(SerializableFunction)}. */
    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return new Write<>(inner.withDataSourceProviderFn(dataSourceProviderFn));
    }

    /** See {@link WriteVoid#withStatement(String)}. */
    public Write<T> withStatement(String statement) {
      return new Write<>(inner.withStatement(statement));
    }

    /** See {@link WriteVoid#withPreparedStatementSetter(PreparedStatementSetter)}. */
    public Write<T> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return new Write<>(inner.withPreparedStatementSetter(setter));
    }

    /** See {@link WriteVoid#withBatchSize(long)}. */
    public Write<T> withBatchSize(long batchSize) {
      return new Write<>(inner.withBatchSize(batchSize));
    }

    /** See {@link WriteVoid#withRetryStrategy(RetryStrategy)}. */
    public Write<T> withRetryStrategy(RetryStrategy retryStrategy) {
      return new Write<>(inner.withRetryStrategy(retryStrategy));
    }

    /** See {@link WriteVoid#withRetryConfiguration(RetryConfiguration)}. */
    public Write<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      return new Write<>(inner.withRetryConfiguration(retryConfiguration));
    }

    /** See {@link WriteVoid#withTable(String)}. */
    public Write<T> withTable(String table) {
      return new Write<>(inner.withTable(table));
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

    /**
     * Returns {@link WriteWithResults} transform that could return a specific result.
     *
     * <p>See {@link WriteWithResults}
     */
    public <V extends JdbcWriteResult> WriteWithResults<T, V> withWriteResults(
        RowMapper<V> rowMapper) {
      return new AutoValue_JdbcIO_WriteWithResults.Builder<T, V>()
          .setRowMapper(rowMapper)
          .setRetryStrategy(inner.getRetryStrategy())
          .setRetryConfiguration(inner.getRetryConfiguration())
          .setDataSourceProviderFn(inner.getDataSourceProviderFn())
          .setPreparedStatementSetter(inner.getPreparedStatementSetter())
          .setStatement(inner.getStatement())
          .setTable(inner.getTable())
          .setAutoSharding(inner.getAutoSharding())
          .build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    @Override
    public PDone expand(PCollection<T> input) {
      inner.expand(input);
      return PDone.in(input.getPipeline());
    }
  }

  /* The maximum number of elements that will be included in a batch. */

  static <T> PCollection<Iterable<T>> batchElements(
      PCollection<T> input, @Nullable Boolean withAutoSharding, long batchSize) {
    PCollection<Iterable<T>> iterables;
    if (input.isBounded() == IsBounded.UNBOUNDED) {
      PCollection<KV<String, T>> keyedInput = input.apply(WithKeys.<String, T>of(""));
      GroupIntoBatches<String, T> groupTransform =
          GroupIntoBatches.<String, T>ofSize(batchSize)
              .withMaxBufferingDuration(Duration.millis(200));
      if (withAutoSharding != null && withAutoSharding) {
        // unbounded and withAutoSharding enabled, group into batches with shardedKey
        iterables = keyedInput.apply(groupTransform.withShardedKey()).apply(Values.create());
      } else {
        // unbounded and without auto sharding, group into batches of assigned max size
        iterables = keyedInput.apply(groupTransform).apply(Values.create());
      }
    } else {
      iterables =
          input.apply(
              ParDo.of(
                  new DoFn<T, Iterable<T>>() {
                    @Nullable List<T> outputList;

                    @ProcessElement
                    public void process(ProcessContext c) {
                      if (outputList == null) {
                        outputList = new ArrayList<>();
                      }
                      outputList.add(c.element());
                      if (outputList.size() > batchSize) {
                        c.output(outputList);
                        outputList = null;
                      }
                    }

                    @FinishBundle
                    public void finish(FinishBundleContext c) {
                      if (outputList != null && outputList.size() > 0) {
                        c.output(outputList, Instant.now(), GlobalWindow.INSTANCE);
                      }
                      outputList = null;
                    }
                  }));
    }
    return iterables;
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

  /**
   * A {@link PTransform} to write to a JDBC datasource. Executes statements one by one.
   *
   * <p>The INSERT, UPDATE, and DELETE commands sometimes have an optional RETURNING clause that
   * supports obtaining data from modified rows while they are being manipulated. Output {@link
   * PCollection} of this transform is a collection of such returning results mapped by {@link
   * RowMapper}.
   */
  @AutoValue
  public abstract static class WriteWithResults<T, V extends JdbcWriteResult>
      extends PTransform<PCollection<T>, PCollection<V>> {
    abstract @Nullable Boolean getAutoSharding();

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    abstract @Nullable ValueProvider<String> getStatement();

    abstract @Nullable PreparedStatementSetter<T> getPreparedStatementSetter();

    abstract @Nullable RetryStrategy getRetryStrategy();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable String getTable();

    abstract @Nullable RowMapper<V> getRowMapper();

    abstract Builder<T, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T, V extends JdbcWriteResult> {
      abstract Builder<T, V> setDataSourceProviderFn(
          @Nullable SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T, V> setAutoSharding(@Nullable Boolean autoSharding);

      abstract Builder<T, V> setStatement(@Nullable ValueProvider<String> statement);

      abstract Builder<T, V> setPreparedStatementSetter(
          @Nullable PreparedStatementSetter<T> setter);

      abstract Builder<T, V> setRetryStrategy(@Nullable RetryStrategy deadlockPredicate);

      abstract Builder<T, V> setRetryConfiguration(@Nullable RetryConfiguration retryConfiguration);

      abstract Builder<T, V> setTable(@Nullable String table);

      abstract Builder<T, V> setRowMapper(RowMapper<V> rowMapper);

      abstract WriteWithResults<T, V> build();
    }

    public WriteWithResults<T, V> withDataSourceConfiguration(DataSourceConfiguration config) {
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    public WriteWithResults<T, V> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    public WriteWithResults<T, V> withStatement(String statement) {
      return withStatement(ValueProvider.StaticValueProvider.of(statement));
    }

    public WriteWithResults<T, V> withStatement(ValueProvider<String> statement) {
      return toBuilder().setStatement(statement).build();
    }

    public WriteWithResults<T, V> withPreparedStatementSetter(PreparedStatementSetter<T> setter) {
      return toBuilder().setPreparedStatementSetter(setter).build();
    }

    /** If true, enables using a dynamically determined number of shards to write. */
    public WriteWithResults<T, V> withAutoSharding() {
      return toBuilder().setAutoSharding(true).build();
    }

    /**
     * When a SQL exception occurs, {@link Write} uses this {@link RetryStrategy} to determine if it
     * will retry the statements. If {@link RetryStrategy#apply(SQLException)} returns {@code true},
     * then {@link Write} retries the statements.
     */
    public WriteWithResults<T, V> withRetryStrategy(RetryStrategy retryStrategy) {
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
     *    .withReturningResults(...)
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
     *    .withReturningResults(...)
     *    .withDataSourceConfiguration(...)
     *    .withRetryStrategy(...)
     *    .withRetryConfiguration(JdbcIO.RetryConfiguration.
     *        create(5, null, null)
     *
     * }</pre>
     */
    public WriteWithResults<T, V> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration can not be null");
      return toBuilder().setRetryConfiguration(retryConfiguration).build();
    }

    public WriteWithResults<T, V> withTable(String table) {
      checkArgument(table != null, "table name can not be null");
      return toBuilder().setTable(table).build();
    }

    public WriteWithResults<T, V> withRowMapper(RowMapper<V> rowMapper) {
      checkArgument(rowMapper != null, "result set getter can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    @Override
    public PCollection<V> expand(PCollection<T> input) {
      checkArgument(getStatement() != null, "withStatement() is required");
      checkArgument(
          getPreparedStatementSetter() != null, "withPreparedStatementSetter() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
      @Nullable Boolean autoSharding = getAutoSharding();
      checkArgument(
          autoSharding == null || (autoSharding && input.isBounded() != IsBounded.UNBOUNDED),
          "Autosharding is only supported for streaming pipelines.");

      PCollection<Iterable<T>> iterables =
          JdbcIO.<T>batchElements(input, autoSharding, DEFAULT_BATCH_SIZE);
      return iterables.apply(
          ParDo.of(
              new WriteFn<T, V>(
                  WriteFnSpec.builder()
                      .setRetryStrategy(getRetryStrategy())
                      .setDataSourceProviderFn(getDataSourceProviderFn())
                      .setPreparedStatementSetter(getPreparedStatementSetter())
                      .setRowMapper(getRowMapper())
                      .setStatement(getStatement())
                      .setRetryConfiguration(getRetryConfiguration())
                      .setReturnResults(true)
                      .setBatchSize(1L)
                      .build())));
    }
  }

  /**
   * A {@link PTransform} to write to a JDBC datasource. Executes statements in a batch, and returns
   * a trivial result.
   */
  @AutoValue
  public abstract static class WriteVoid<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    abstract @Nullable Boolean getAutoSharding();

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
      abstract Builder<T> setAutoSharding(Boolean autoSharding);

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

    /** If true, enables using a dynamically determined number of shards to write. */
    public WriteVoid<T> withAutoSharding() {
      return toBuilder().setAutoSharding(true).build();
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
      WriteVoid<T> spec = this;
      checkArgument(
          (spec.getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
      // fixme: validate invalid table input
      if (input.hasSchema() && !spec.hasStatementAndSetter()) {
        checkArgument(spec.getTable() != null, "table cannot be null if statement is not provided");
        List<SchemaUtil.FieldWithIndex> fields = spec.getFilteredFields(input.getSchema());
        spec =
            spec.toBuilder()
                .setStatement(spec.generateStatement(fields))
                .setPreparedStatementSetter(
                    new AutoGeneratedPreparedStatementSetter(fields, input.getToRowFunction()))
                .build();
      } else {
        checkArgument(spec.getStatement() != null, "withStatement() is required");
        checkArgument(
            spec.getPreparedStatementSetter() != null, "withPreparedStatementSetter() is required");
      }

      PCollection<Iterable<T>> iterables =
          JdbcIO.<T>batchElements(input, getAutoSharding(), getBatchSize());

      return iterables
          .apply(
              ParDo.of(
                  new WriteFn<T, Void>(
                      WriteFnSpec.builder()
                          .setRetryConfiguration(spec.getRetryConfiguration())
                          .setRetryStrategy(spec.getRetryStrategy())
                          .setPreparedStatementSetter(spec.getPreparedStatementSetter())
                          .setDataSourceProviderFn(spec.getDataSourceProviderFn())
                          .setTable(spec.getTable())
                          .setStatement(spec.getStatement())
                          .setBatchSize(spec.getBatchSize())
                          .setReturnResults(false)
                          .build())))
          .setCoder(VoidCoder.of());
    }

    private StaticValueProvider<String> generateStatement(List<SchemaUtil.FieldWithIndex> fields) {
      return StaticValueProvider.of(
          JdbcUtil.generateStatement(
              checkStateNotNull(getTable()),
              fields.stream().map(FieldWithIndex::getField).collect(Collectors.toList())));
    }

    // Spotbugs seems to not understand the multi-statement try-with-resources
    @SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
    private List<SchemaUtil.FieldWithIndex> getFilteredFields(Schema schema) {
      Schema tableSchema;

      try (Connection connection =
              checkStateNotNull(getDataSourceProviderFn()).apply(null).getConnection();
          PreparedStatement statement =
              connection.prepareStatement(String.format("SELECT * FROM %s", getTable()))) {
        ResultSetMetaData metadata =
            checkStateNotNull(statement.getMetaData(), "could not get statement metadata");
        tableSchema = SchemaUtil.toBeamSchema(metadata);
      } catch (SQLException e) {
        throw new RuntimeException("Error while determining columns from table: " + getTable(), e);
      }

      checkState(
          tableSchema.getFieldCount() >= schema.getFieldCount(),
          String.format(
              "Input schema has more fields (%s) than actual table (%s).%n\t"
                  + "Input schema fields: %s | Table fields: %s",
              tableSchema.getFieldCount(),
              schema.getFieldCount(),
              schema.getFields().stream()
                  .map(Schema.Field::getName)
                  .collect(Collectors.joining(", ")),
              tableSchema.getFields().stream()
                  .map(Schema.Field::getName)
                  .collect(Collectors.joining(", "))));

      // filter out missing fields from output table
      List<Schema.Field> missingFields =
          tableSchema.getFields().stream()
              .filter(
                  line ->
                      schema.getFields().stream()
                          .noneMatch(s -> s.getName().equalsIgnoreCase(line.getName())))
              .collect(Collectors.toList());

      // allow insert only if missing fields are nullable
      checkState(
          !checkNullabilityForFields(missingFields),
          "Non nullable fields are not allowed without a matching schema. "
              + "Fields %s were in the destination table but not in the input schema.",
          missingFields);

      List<SchemaUtil.FieldWithIndex> tableFilteredFields = new ArrayList<>();

      for (Schema.Field tableField : tableSchema.getFields()) {
        for (Schema.Field f : schema.getFields()) {
          if (SchemaUtil.compareSchemaField(tableField, f)) {
            tableFilteredFields.add(FieldWithIndex.of(tableField, schema.getFields().indexOf(f)));
            break;
          }
        }
      }

      checkState(
          tableFilteredFields.size() == schema.getFieldCount(),
          "Provided schema doesn't match with database schema."
              + " Table has fields: %s"
              + " while provided schema has fields: %s",
          tableFilteredFields.stream()
              .map(f -> f.getIndex().toString() + "-" + f.getField().getName())
              .collect(Collectors.joining(",")),
          schema.getFieldNames().toString());

      return tableFilteredFields;
    }

    /**
     * A {@link org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter} implementation that
     * calls related setters on prepared statement.
     */
    private class AutoGeneratedPreparedStatementSetter implements PreparedStatementSetter<T> {

      private final List<SchemaUtil.FieldWithIndex> fields;
      private final SerializableFunction<T, Row> toRowFn;
      private final List<PreparedStatementSetCaller> preparedStatementFieldSetterList =
          new ArrayList<>();

      AutoGeneratedPreparedStatementSetter(
          List<SchemaUtil.FieldWithIndex> fieldsWithIndex, SerializableFunction<T, Row> toRowFn) {
        this.fields = fieldsWithIndex;
        this.toRowFn = toRowFn;
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

    private boolean hasStatementAndSetter() {
      return getStatement() != null && getPreparedStatementSetter() != null;
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
            @SuppressWarnings("nullness") // apache.commons.dbcp2 not annotated
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

  /**
   * {@link DoFn} class to write results data to a JDBC sink. It supports writing rows one by one
   * (and returning individual results) - or by batch.
   *
   * @param <T>
   * @param <V>
   */
  static class WriteFn<T, V> extends DoFn<Iterable<T>, V> {

    @AutoValue
    abstract static class WriteFnSpec<T, V> implements Serializable, HasDisplayData {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder
            .addIfNotNull(
                DisplayData.item(
                    "dataSourceProviderFn",
                    getDataSourceProviderFn() == null
                        ? "null"
                        : getDataSourceProviderFn().getClass().getName()))
            .addIfNotNull(DisplayData.item("statement", getStatement()))
            .addIfNotNull(
                DisplayData.item(
                    "preparedStatementSetter",
                    getPreparedStatementSetter() == null
                        ? "null"
                        : getPreparedStatementSetter().getClass().getName()))
            .addIfNotNull(
                DisplayData.item(
                    "retryConfiguration",
                    getRetryConfiguration() == null
                        ? "null"
                        : getRetryConfiguration().getClass().getName()))
            .addIfNotNull(DisplayData.item("table", getTable()))
            .addIfNotNull(
                DisplayData.item(
                    "rowMapper",
                    getRowMapper() == null ? "null" : getRowMapper().getClass().toString()))
            .addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      }

      @Pure
      abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

      @Pure
      abstract @Nullable ValueProvider<String> getStatement();

      @Pure
      abstract @Nullable PreparedStatementSetter<T> getPreparedStatementSetter();

      @Pure
      abstract @Nullable RetryStrategy getRetryStrategy();

      @Pure
      abstract @Nullable RetryConfiguration getRetryConfiguration();

      @Pure
      abstract @Nullable String getTable();

      @Pure
      abstract @Nullable RowMapper<V> getRowMapper();

      @Pure
      abstract @Nullable Long getBatchSize();

      @Pure
      abstract Boolean getReturnResults();

      @Pure
      static Builder builder() {
        return new AutoValue_JdbcIO_WriteFn_WriteFnSpec.Builder();
      }

      @AutoValue.Builder
      abstract static class Builder<T, V> {
        abstract Builder<T, V> setDataSourceProviderFn(
            @Nullable SerializableFunction<Void, DataSource> fn);

        abstract Builder<T, V> setStatement(@Nullable ValueProvider<String> statement);

        abstract Builder<T, V> setPreparedStatementSetter(
            @Nullable PreparedStatementSetter<T> setter);

        abstract Builder<T, V> setRetryStrategy(@Nullable RetryStrategy retryStrategy);

        abstract Builder<T, V> setRetryConfiguration(
            @Nullable RetryConfiguration retryConfiguration);

        abstract Builder<T, V> setTable(@Nullable String table);

        abstract Builder<T, V> setRowMapper(@Nullable RowMapper<V> rowMapper);

        abstract Builder<T, V> setBatchSize(@Nullable Long batchSize);

        abstract Builder<T, V> setReturnResults(Boolean returnResults);

        abstract WriteFnSpec<T, V> build();
      }
    }

    private static final Distribution RECORDS_PER_BATCH =
        Metrics.distribution(WriteFn.class, "records_per_jdbc_batch");
    private static final Distribution MS_PER_BATCH =
        Metrics.distribution(WriteFn.class, "milliseconds_per_batch");

    private final WriteFnSpec<T, V> spec;
    private @Nullable DataSource dataSource;
    private @Nullable Connection connection;
    private @Nullable PreparedStatement preparedStatement;
    private static @Nullable FluentBackoff retryBackOff;

    public WriteFn(WriteFnSpec<T, V> spec) {
      this.spec = spec;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.add(
          DisplayData.item(
              "query", preparedStatement == null ? "null" : preparedStatement.toString()));
      builder.add(
          DisplayData.item("dataSource", dataSource == null ? "null" : dataSource.toString()));
      builder.add(DisplayData.item("spec", spec == null ? "null" : spec.toString()));
    }

    @Setup
    public void setup() {
      dataSource = checkStateNotNull(spec.getDataSourceProviderFn()).apply(null);
      RetryConfiguration retryConfiguration = checkStateNotNull(spec.getRetryConfiguration());

      retryBackOff =
          FluentBackoff.DEFAULT
              .withInitialBackoff(checkStateNotNull(retryConfiguration.getInitialDuration()))
              .withMaxCumulativeBackoff(checkStateNotNull(retryConfiguration.getMaxDuration()))
              .withMaxRetries(retryConfiguration.getMaxAttempts());
    }

    private Connection getConnection() throws SQLException {
      if (connection == null) {
        connection = checkStateNotNull(dataSource).getConnection();
        connection.setAutoCommit(false);
        preparedStatement =
            connection.prepareStatement(checkStateNotNull(spec.getStatement()).get());
      }
      return connection;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      executeBatch(context, context.element());
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      // We pass a null context because we only execute a final batch for WriteVoid cases.
      cleanUpStatementAndConnection();
    }

    @Teardown
    public void tearDown() throws Exception {
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

    private void executeBatch(ProcessContext context, Iterable<T> records)
        throws SQLException, IOException, InterruptedException {
      Long startTimeNs = System.nanoTime();
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff = checkStateNotNull(retryBackOff).backoff();
      RetryStrategy retryStrategy = checkStateNotNull(spec.getRetryStrategy());
      while (true) {
        try (PreparedStatement preparedStatement =
            getConnection().prepareStatement(checkStateNotNull(spec.getStatement()).get())) {
          try {
            // add each record in the statement batch
            int recordsInBatch = 0;
            for (T record : records) {
              processRecord(record, preparedStatement, context);
              recordsInBatch += 1;
            }
            if (!spec.getReturnResults()) {
              // execute the batch
              preparedStatement.executeBatch();
              // commit the changes
              getConnection().commit();
            }
            RECORDS_PER_BATCH.update(recordsInBatch);
            MS_PER_BATCH.update(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs));
            break;
          } catch (SQLException exception) {
            LOG.trace(
                "SQL exception thrown while writing to JDBC database: {}", exception.getMessage());
            if (!retryStrategy.apply(exception)) {
              throw exception;
            }
            LOG.warn("Deadlock detected, retrying", exception);
            // clean up the statement batch and the connection state
            preparedStatement.clearBatch();
            if (connection != null) {
              connection.rollback();
            }
            if (!BackOffUtils.next(sleeper, backoff)) {
              // we tried the max number of times
              throw exception;
            }
          }
        }
      }
    }

    private void processRecord(T record, PreparedStatement preparedStatement, ProcessContext c) {
      try {
        preparedStatement.clearParameters();
        checkStateNotNull(spec.getPreparedStatementSetter())
            .setParameters(record, preparedStatement);
        if (spec.getReturnResults()) {
          RowMapper<V> rowMapper = checkStateNotNull(spec.getRowMapper());
          // execute the statement
          preparedStatement.execute();
          // commit the changes
          getConnection().commit();
          c.output(rowMapper.mapRow(preparedStatement.getResultSet()));
        } else {
          preparedStatement.addBatch();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
