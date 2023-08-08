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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DelegatingStatement;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on SingleStoreDB.
 *
 * <h3>Reading from SingleStoreDB datasource</h3>
 *
 * <p>SingleStoreIO source returns a bounded collection of {@code T} as a {@code PCollection<T>}. T
 * is the type returned by the provided {@link RowMapper}.
 *
 * <p>To configure the SingleStoreDB source, you have to provide a {@link DataSourceConfiguration}
 * using {@link DataSourceConfiguration#create(String)}(endpoint). Optionally, {@link
 * DataSourceConfiguration#withUsername(String)} and {@link
 * DataSourceConfiguration#withPassword(String)} allows you to define username and password.
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline.apply(SingleStoreIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *        .withUsername("username")
 *        .withPassword("password"))
 *   .withQuery("select id, name from Person")
 *   .withRowMapper(new RowMapper<KV<Integer, String>>() {
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
 * pipeline.apply(SingleStoreIO.<KV<Integer, String>>read()
 *   .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306"))
 *   .withQuery("select id,name from Person where name = ?")
 *   .withStatementPreparator(new StatementPreparator() {
 *     public void setParameters(PreparedStatement preparedStatement) throws Exception {
 *       preparedStatement.setString(1, "Darwin");
 *     }
 *   })
 *   .withRowMapper(new RowMapper<KV<Integer, String>>() {
 *     public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *       return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *     }
 *   })
 * );
 * }</pre>
 *
 * <h4>Parallel reading from a SingleStoreDB datasource</h4>
 *
 * <p>SingleStoreIO supports partitioned reading of all data from a table. To enable this, use
 * {@link SingleStoreIO#readWithPartitions()}. This way of data reading is preferred because of the
 * performance reasons.
 *
 * <p>The following example shows usage of {@link SingleStoreIO#readWithPartitions()}
 *
 * <pre>{@code
 * pipeline.apply(SingleStoreIO.<Row>readWithPartitions()
 *  .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *       .withUsername("username")
 *       .withPassword("password"))
 *  .withTable("Person")
 *  .withRowMapper(new RowMapper<KV<Integer, String>>() {
 *    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
 *      return KV.of(resultSet.getInt(1), resultSet.getString(2));
 *    }
 *  })
 * );
 * }</pre>
 *
 * <h3>Writing to SingleStoreDB datasource</h3>
 *
 * <p>SingleStoreIO supports writing records into a database. It writes a {@link PCollection} to the
 * database by converting data to CSV and sending it to the database with LOAD DATA query.
 *
 * <p>Like the source, to configure the sink, you have to provide a {@link DataSourceConfiguration}.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(SingleStoreIO.<KV<Integer, String>>write()
 *      .withDataSourceConfiguration(DataSourceConfiguration.create("hostname:3306")
 *          .withUsername("username")
 *          .withPassword("password"))
 *      .withStatement("insert into Person values(?, ?)")
 *      .withUserDataMapper(new UserDataMapper<KV<Integer, String>>() {
 *        @Override
 *        public List<String> mapRow(KV<Integer, String> element) {
 *          List<String> res = new ArrayList<>();
 *          res.add(element.getKey().toString());
 *          res.add(element.getValue());
 *          return res;
 *        }
 *      })
 *    );
 * }</pre>
 */
public class SingleStoreIO {
  /**
   * Read data from a SingleStoreDB datasource.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return new AutoValue_SingleStoreIO_Read.Builder<T>().setOutputParallelization(true).build();
  }

  /** Read Beam {@link Row}s from a SingleStoreDB datasource. */
  public static Read<Row> readRows() {
    return new AutoValue_SingleStoreIO_Read.Builder<Row>()
        .setRowMapper(new SingleStoreDefaultRowMapper())
        .setOutputParallelization(true)
        .build();
  }

  /**
   * Like {@link #read}, but executes multiple instances of the query on the same table for each
   * database partition.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> ReadWithPartitions<T> readWithPartitions() {
    return new AutoValue_SingleStoreIO_ReadWithPartitions.Builder<T>().build();
  }

  /**
   * Like {@link #readRows}, but executes multiple instances of the query on the same table for each
   * database partition.
   */
  public static ReadWithPartitions<Row> readWithPartitionsRows() {
    return new AutoValue_SingleStoreIO_ReadWithPartitions.Builder<Row>()
        .setRowMapper(new SingleStoreDefaultRowMapper())
        .build();
  }

  /**
   * Write data to a SingleStoreDB datasource.
   *
   * @param <T> Type of the data to be written.
   */
  public static <T> Write<T> write() {
    return new AutoValue_SingleStoreIO_Write.Builder<T>().build();
  }

  /** Write Beam {@link Row}s to a SingleStoreDB datasource. */
  public static Write<Row> writeRows() {
    return new AutoValue_SingleStoreIO_Write.Builder<Row>()
        .setUserDataMapper(new SingleStoreDefaultUserDataMapper())
        .build();
  }

  /**
   * An interface used by {@link Read} and {@link ReadWithPartitions} for converting each row of the
   * {@link ResultSet} into an element of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface RowMapper<T> extends Serializable {
    T mapRow(ResultSet resultSet) throws Exception;
  }

  /**
   * A RowMapper that requires initialization. init method is called during pipeline construction
   * time.
   */
  public interface RowMapperWithInit<T> extends RowMapper<T> {
    void init(ResultSetMetaData resultSetMetaData) throws Exception;
  }

  /** A RowMapper that provides a Coder for resulting PCollection. */
  public interface RowMapperWithCoder<T> extends RowMapper<T> {
    Coder<T> getCoder() throws Exception;
  }

  /**
   * An interface used by the SingleStoreIO {@link Read} to set the parameters of the {@link
   * PreparedStatement}.
   */
  @FunctionalInterface
  public interface StatementPreparator extends Serializable {
    void setParameters(PreparedStatement preparedStatement) throws Exception;
  }

  /**
   * An interface used by the SingleStoreIO {@link Write} to map a data from each element of {@link
   * org.apache.beam.sdk.values.PCollection} to a List of Strings. Returned values are used to send
   * LOAD DATA SQL queries to the database.
   */
  @FunctionalInterface
  public interface UserDataMapper<T> extends Serializable {
    List<String> mapRow(T element);
  }

  /**
   * A POJO describing a SingleStoreDB {@link DataSource} by providing all properties needed to
   * create it.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    abstract @Nullable String getEndpoint();

    abstract @Nullable String getUsername();

    abstract @Nullable String getPassword();

    abstract @Nullable String getDatabase();

    abstract @Nullable String getConnectionProperties();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEndpoint(String endpoint);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setDatabase(String database);

      abstract Builder setConnectionProperties(String connectionProperties);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create(String endpoint) {
      checkNotNull(endpoint, "endpoint can not be null");
      return new AutoValue_SingleStoreIO_DataSourceConfiguration.Builder()
          .setEndpoint(endpoint)
          .build();
    }

    public DataSourceConfiguration withUsername(String username) {
      checkNotNull(username, "username can not be null");
      return builder().setUsername(username).build();
    }

    public DataSourceConfiguration withPassword(String password) {
      checkNotNull(password, "password can not be null");
      return builder().setPassword(password).build();
    }

    public DataSourceConfiguration withDatabase(String database) {
      checkNotNull(database, "database can not be null");
      return builder().setDatabase(database).build();
    }

    /**
     * Sets the connection properties passed to driver.connect(...). Format of the string must be
     * [propertyName=property;]*
     *
     * <p>NOTE - The "user" and "password" properties can be add via {@link #withUsername(String)},
     * {@link #withPassword(String)}, so they do not need to be included here.
     *
     * <p>Full list of supported properties can be found here {@link <a
     * href="https://docs.singlestore.com/managed-service/en/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver.html#connection-string-parameters">...</a>}
     */
    public DataSourceConfiguration withConnectionProperties(String connectionProperties) {
      checkNotNull(connectionProperties, "connectionProperties can not be null");
      return builder().setConnectionProperties(connectionProperties).build();
    }

    public static void populateDisplayData(
        @Nullable DataSourceConfiguration dataSourceConfiguration, DisplayData.Builder builder) {
      if (dataSourceConfiguration != null) {
        builder.addIfNotNull(DisplayData.item("endpoint", dataSourceConfiguration.getEndpoint()));
        builder.addIfNotNull(DisplayData.item("username", dataSourceConfiguration.getUsername()));
        builder.addIfNotNull(DisplayData.item("database", dataSourceConfiguration.getDatabase()));
        builder.addIfNotNull(
            DisplayData.item(
                "connectionProperties", dataSourceConfiguration.getConnectionProperties()));
      }
    }

    public DataSource getDataSource() {
      String endpoint = getEndpoint();
      Preconditions.checkArgumentNotNull(endpoint, "endpoint can not be null");
      String database = SingleStoreUtil.getArgumentWithDefault(getDatabase(), "");
      String connectionProperties =
          SingleStoreUtil.getArgumentWithDefault(getConnectionProperties(), "");
      connectionProperties += (connectionProperties.isEmpty() ? "" : ";") + "allowLocalInfile=TRUE";
      connectionProperties +=
          String.format(
              ";connectionAttributes=_connector_name:%s,_connector_version:%s,_product_version:%s",
              "Apache Beam SingleStoreDB I/O",
              ReleaseInfo.getReleaseInfo().getVersion(),
              ReleaseInfo.getReleaseInfo().getVersion());
      String username = getUsername();
      String password = getPassword();

      BasicDataSource basicDataSource = new BasicDataSource();
      basicDataSource.setDriverClassName("com.singlestore.jdbc.Driver");
      basicDataSource.setUrl(String.format("jdbc:singlestore://%s/%s", endpoint, database));

      if (username != null) {
        basicDataSource.setUsername(username);
      }
      if (password != null) {
        basicDataSource.setPassword(password);
      }
      basicDataSource.setConnectionProperties(connectionProperties);

      return basicDataSource;
    }
  }

  /**
   * A {@link PTransform} for reading data from SingleStoreDB. It is used by {@link
   * SingleStoreIO#read()}.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(Read.class);

    abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

    abstract @Nullable String getQuery();

    abstract @Nullable String getTable();

    abstract @Nullable StatementPreparator getStatementPreparator();

    abstract @Nullable Boolean getOutputParallelization();

    abstract @Nullable RowMapper<T> getRowMapper();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(
          DataSourceConfiguration dataSourceConfiguration);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setStatementPreparator(StatementPreparator statementPreparator);

      abstract Builder<T> setOutputParallelization(Boolean outputParallelization);

      abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

      abstract Read<T> build();
    }

    public Read<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      checkNotNull(config, "dataSourceConfiguration can not be null");
      return toBuilder().setDataSourceConfiguration(config).build();
    }

    public Read<T> withQuery(String query) {
      checkNotNull(query, "query can not be null");
      return toBuilder().setQuery(query).build();
    }

    public Read<T> withTable(String table) {
      checkNotNull(table, "table can not be null");
      return toBuilder().setTable(table).build();
    }

    public Read<T> withStatementPreparator(StatementPreparator statementPreparator) {
      checkNotNull(statementPreparator, "statementPreparator can not be null");
      return toBuilder().setStatementPreparator(statementPreparator).build();
    }

    /**
     * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
     * default is to parallelize and should only be changed if this is known to be unnecessary.
     */
    public Read<T> withOutputParallelization(Boolean outputParallelization) {
      checkNotNull(outputParallelization, "outputParallelization can not be null");
      return toBuilder().setOutputParallelization(outputParallelization).build();
    }

    public Read<T> withRowMapper(RowMapper<T> rowMapper) {
      checkNotNull(rowMapper, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
      Preconditions.checkArgumentNotNull(
          dataSourceConfiguration, "withDataSourceConfiguration() is required");
      RowMapper<T> rowMapper = getRowMapper();
      Preconditions.checkArgumentNotNull(rowMapper, "withRowMapper() is required");
      String actualQuery = SingleStoreUtil.getSelectQuery(getTable(), getQuery());

      if (rowMapper instanceof RowMapperWithInit) {
        try {
          ((RowMapperWithInit<?>) rowMapper)
              .init(getResultSetMetadata(dataSourceConfiguration, actualQuery));
        } catch (Exception e) {
          throw new SingleStoreRowMapperInitializationException(e);
        }
      }

      Coder<T> coder =
          SingleStoreUtil.inferCoder(
              rowMapper,
              input.getPipeline().getCoderRegistry(),
              input.getPipeline().getSchemaRegistry(),
              LOG);

      PCollection<T> output =
          input
              .apply(Create.of((Void) null))
              .apply(
                  ParDo.of(
                      new ReadFn<>(
                          dataSourceConfiguration,
                          actualQuery,
                          getStatementPreparator(),
                          rowMapper)))
              .setCoder(coder);

      if (SingleStoreUtil.getArgumentWithDefault(getOutputParallelization(), true)) {
        output = output.apply(new Reparallelize<>());
      }

      return output;
    }

    public static class SingleStoreRowMapperInitializationException extends RuntimeException {
      SingleStoreRowMapperInitializationException(Throwable cause) {
        super("Failed to initialize RowMapper", cause);
      }
    }

    private static class ReadFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
      DataSourceConfiguration dataSourceConfiguration;
      String query;
      @Nullable StatementPreparator statementPreparator;
      RowMapper<OutputT> rowMapper;

      ReadFn(
          DataSourceConfiguration dataSourceConfiguration,
          String query,
          @Nullable StatementPreparator statementPreparator,
          RowMapper<OutputT> rowMapper) {
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.query = query;
        this.statementPreparator = statementPreparator;
        this.rowMapper = rowMapper;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        DataSource dataSource = dataSourceConfiguration.getDataSource();
        Connection conn = dataSource.getConnection();
        try {
          PreparedStatement stmt = conn.prepareStatement(query);
          try {
            if (statementPreparator != null) {
              statementPreparator.setParameters(stmt);
            }

            ResultSet res = stmt.executeQuery();
            try {
              while (res.next()) {
                context.output(rowMapper.mapRow(res));
              }
            } finally {
              res.close();
            }
          } finally {
            stmt.close();
          }
        } finally {
          conn.close();
        }
      }
    }

    // Reparallelize PTransform is copied from JdbcIO
    // https://github.com/apache/beam/blob/9d118bde5fe5a93c5f559ac3227758aea88185b8/sdks/java/io/jdbc/src/main/java/org/apache/beam/sdk/io/jdbc/JdbcIO.java#L2115
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
        // The current approach is necessary only to support the particular case of SingleStoreIO
        // where
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

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      DataSourceConfiguration.populateDisplayData(getDataSourceConfiguration(), builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("table", getTable()));
      builder.addIfNotNull(
          DisplayData.item(
              "statementPreparator", SingleStoreUtil.getClassNameOrNull(getStatementPreparator())));
      builder.addIfNotNull(DisplayData.item("outputParallelization", getOutputParallelization()));
      builder.addIfNotNull(
          DisplayData.item("rowMapper", SingleStoreUtil.getClassNameOrNull(getRowMapper())));
    }
  }

  /**
   * A {@link PTransform} for reading data from SingleStoreDB. It is used by {@link
   * SingleStoreIO#readWithPartitions()}. {@link ReadWithPartitions} is preferred over {@link Read}
   * because of the performance reasons.
   */
  @AutoValue
  public abstract static class ReadWithPartitions<T> extends PTransform<PBegin, PCollection<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(ReadWithPartitions.class);

    abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

    abstract @Nullable String getQuery();

    abstract @Nullable String getTable();

    abstract @Nullable RowMapper<T> getRowMapper();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(
          DataSourceConfiguration dataSourceConfiguration);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setRowMapper(RowMapper<T> rowMapper);

      abstract ReadWithPartitions<T> build();
    }

    public ReadWithPartitions<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      checkNotNull(config, "dataSourceConfiguration can not be null");
      return toBuilder().setDataSourceConfiguration(config).build();
    }

    public ReadWithPartitions<T> withQuery(String query) {
      checkNotNull(query, "query can not be null");
      return toBuilder().setQuery(query).build();
    }

    public ReadWithPartitions<T> withTable(String table) {
      checkNotNull(table, "table can not be null");
      return toBuilder().setTable(table).build();
    }

    public ReadWithPartitions<T> withRowMapper(RowMapper<T> rowMapper) {
      checkNotNull(rowMapper, "rowMapper can not be null");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
      Preconditions.checkArgumentNotNull(
          dataSourceConfiguration, "withDataSourceConfiguration() is required");
      String database = dataSourceConfiguration.getDatabase();
      Preconditions.checkArgumentNotNull(
          database,
          "withDatabase() is required for DataSourceConfiguration in order to perform readWithPartitions");
      RowMapper<T> rowMapper = getRowMapper();
      Preconditions.checkArgumentNotNull(rowMapper, "withRowMapper() is required");

      String actualQuery = SingleStoreUtil.getSelectQuery(getTable(), getQuery());

      if (rowMapper instanceof RowMapperWithInit) {
        try {
          ((RowMapperWithInit<?>) rowMapper)
              .init(getResultSetMetadata(dataSourceConfiguration, actualQuery));
        } catch (Exception e) {
          throw new Read.SingleStoreRowMapperInitializationException(e);
        }
      }

      Coder<T> coder =
          SingleStoreUtil.inferCoder(
              rowMapper,
              input.getPipeline().getCoderRegistry(),
              input.getPipeline().getSchemaRegistry(),
              LOG);

      return input
          .apply(Create.of((Void) null))
          .apply(
              ParDo.of(
                  new ReadWithPartitions.ReadWithPartitionsFn<>(
                      dataSourceConfiguration, actualQuery, database, rowMapper)))
          .setCoder(coder);
    }

    private static class ReadWithPartitionsFn<ParameterT, OutputT>
        extends DoFn<ParameterT, OutputT> {
      DataSourceConfiguration dataSourceConfiguration;
      String query;
      String database;
      RowMapper<OutputT> rowMapper;

      ReadWithPartitionsFn(
          DataSourceConfiguration dataSourceConfiguration,
          String query,
          String database,
          RowMapper<OutputT> rowMapper) {
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.query = query;
        this.database = database;
        this.rowMapper = rowMapper;
      }

      @ProcessElement
      public void processElement(
          ProcessContext context, RestrictionTracker<OffsetRange, Long> tracker) throws Exception {
        DataSource dataSource = dataSourceConfiguration.getDataSource();
        Connection conn = dataSource.getConnection();
        try {
          for (long partition = tracker.currentRestriction().getFrom();
              tracker.tryClaim(partition);
              partition++) {
            PreparedStatement stmt =
                conn.prepareStatement(
                    String.format("SELECT * FROM (%s) WHERE partition_id()=%d", query, partition));
            try {
              ResultSet res = stmt.executeQuery();
              try {
                while (res.next()) {
                  context.output(rowMapper.mapRow(res));
                }
              } finally {
                res.close();
              }
            } finally {
              stmt.close();
            }
          }
        } finally {
          conn.close();
        }
      }

      @GetInitialRestriction
      public OffsetRange getInitialRange(@Element ParameterT element) throws Exception {
        return new OffsetRange(0L, getNumPartitions());
      }

      @SplitRestriction
      public void splitRange(
          @Element ParameterT element,
          @Restriction OffsetRange range,
          OutputReceiver<OffsetRange> receiver) {
        for (long i = range.getFrom(); i < range.getTo(); i++) {
          receiver.output(new OffsetRange(i, i + 1));
        }
      }

      private int getNumPartitions() throws Exception {
        DataSource dataSource = dataSourceConfiguration.getDataSource();
        Connection conn = dataSource.getConnection();
        try {
          Statement stmt = conn.createStatement();
          try {
            ResultSet res =
                stmt.executeQuery(
                    String.format(
                        "SELECT num_partitions FROM information_schema.DISTRIBUTED_DATABASES WHERE database_name = %s",
                        SingleStoreUtil.escapeString(database)));
            try {
              if (!res.next()) {
                throw new Exception("Failed to get number of partitions in the database");
              }

              return res.getInt(1);
            } finally {
              res.close();
            }
          } finally {
            stmt.close();
          }
        } finally {
          conn.close();
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      DataSourceConfiguration.populateDisplayData(getDataSourceConfiguration(), builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      builder.addIfNotNull(DisplayData.item("table", getTable()));
      builder.addIfNotNull(
          DisplayData.item("rowMapper", SingleStoreUtil.getClassNameOrNull(getRowMapper())));
    }
  }

  private static ResultSetMetaData getResultSetMetadata(
      DataSourceConfiguration dataSourceConfiguration, String query) throws Exception {
    DataSource dataSource = dataSourceConfiguration.getDataSource();
    Connection conn = dataSource.getConnection();
    try {
      PreparedStatement stmt =
          conn.prepareStatement(String.format("SELECT * FROM (%s) LIMIT 0", query));
      try {
        ResultSetMetaData md = stmt.getMetaData();
        if (md == null) {
          throw new Exception("ResultSetMetaData is null");
        }

        return md;
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }
  }

  /**
   * A {@link PTransform} for writing data to SingleStoreDB. It is used by {@link
   * SingleStoreIO#write()}.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PCollection<Integer>> {

    private static final int DEFAULT_BATCH_SIZE = 100000;
    private static final int BUFFER_SIZE = 524288;

    abstract @Nullable DataSourceConfiguration getDataSourceConfiguration();

    abstract @Nullable String getTable();

    abstract @Nullable Integer getBatchSize();

    abstract @Nullable UserDataMapper<T> getUserDataMapper();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceConfiguration(
          DataSourceConfiguration dataSourceConfiguration);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setBatchSize(Integer batchSize);

      abstract Builder<T> setUserDataMapper(UserDataMapper<T> userDataMapper);

      abstract Write<T> build();
    }

    public Write<T> withDataSourceConfiguration(DataSourceConfiguration config) {
      checkNotNull(config, "dataSourceConfiguration can not be null");
      return toBuilder().setDataSourceConfiguration(config).build();
    }

    public Write<T> withTable(String table) {
      checkNotNull(table, "table can not be null");
      return toBuilder().setTable(table).build();
    }

    public Write<T> withUserDataMapper(UserDataMapper<T> userDataMapper) {
      checkNotNull(userDataMapper, "userDataMapper can not be null");
      return toBuilder().setUserDataMapper(userDataMapper).build();
    }

    /**
     * Provide a maximum number of rows that is written by one SQL statement. Default is 100000.
     *
     * @param batchSize maximum number of rows that is written by one SQL statement
     */
    public Write<T> withBatchSize(Integer batchSize) {
      checkNotNull(batchSize, "batchSize can not be null");
      return toBuilder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<Integer> expand(PCollection<T> input) {
      DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration();
      Preconditions.checkArgumentNotNull(
          dataSourceConfiguration, "withDataSourceConfiguration() is required");
      String table = getTable();
      Preconditions.checkArgumentNotNull(table, "withTable() is required");
      UserDataMapper<T> userDataMapper = getUserDataMapper();
      Preconditions.checkArgumentNotNull(userDataMapper, "withUserDataMapper() is required");
      int batchSize = SingleStoreUtil.getArgumentWithDefault(getBatchSize(), DEFAULT_BATCH_SIZE);
      checkArgument(batchSize > 0, "batchSize should be greater then 0");

      return input
          .apply(
              ParDo.of(
                  new DoFn<T, List<String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) throws Exception {
                      context.output(userDataMapper.mapRow(context.element()));
                    }
                  }))
          .setCoder(ListCoder.of(StringUtf8Coder.of()))
          .apply(ParDo.of(new BatchFn<>(batchSize)))
          .apply(ParDo.of(new WriteFn(dataSourceConfiguration, table)));
    }

    private static class BatchFn<ParameterT> extends DoFn<ParameterT, Iterable<ParameterT>> {
      List<ParameterT> batch = new ArrayList<>();
      int batchSize;

      BatchFn(int batchSize) {
        this.batchSize = batchSize;
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        batch.add(context.element());
        if (batch.size() >= batchSize) {
          context.output(batch);
          batch = new ArrayList<>();
        }
      }

      @FinishBundle
      public void finish(FinishBundleContext context) {
        if (batch.size() > 0) {
          context.output(batch, Instant.now(), GlobalWindow.INSTANCE);
          batch = new ArrayList<>();
        }
      }
    }

    private static class WriteFn extends DoFn<Iterable<List<String>>, Integer> {
      DataSourceConfiguration dataSourceConfiguration;
      String table;

      WriteFn(DataSourceConfiguration dataSourceConfiguration, String table) {
        this.dataSourceConfiguration = dataSourceConfiguration;
        this.table = table;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        DataSource dataSource = dataSourceConfiguration.getDataSource();

        Connection conn = dataSource.getConnection();
        try {
          Statement stmt = conn.createStatement();
          try (PipedOutputStream baseStream = new PipedOutputStream();
              InputStream inputStream = new PipedInputStream(baseStream, BUFFER_SIZE)) {
            ((com.singlestore.jdbc.Statement) ((DelegatingStatement) stmt).getInnermostDelegate())
                .setNextLocalInfileInputStream(inputStream);

            final Exception[] writeException = new Exception[1];

            Thread dataWritingThread =
                new Thread(
                    new Runnable() {
                      @Override
                      public void run() {
                        try {
                          Iterable<List<String>> rows = context.element();
                          for (List<String> row : rows) {
                            for (int i = 0; i < row.size(); i++) {
                              String cell = row.get(i);
                              if (cell.indexOf('\\') != -1) {
                                cell = cell.replace("\\", "\\\\");
                              }
                              if (cell.indexOf('\n') != -1) {
                                cell = cell.replace("\n", "\\n");
                              }
                              if (cell.indexOf('\t') != -1) {
                                cell = cell.replace("\t", "\\t");
                              }

                              baseStream.write(cell.getBytes(StandardCharsets.UTF_8));
                              if (i + 1 == row.size()) {
                                baseStream.write('\n');
                              } else {
                                baseStream.write('\t');
                              }
                            }
                          }

                          baseStream.close();
                        } catch (IOException e) {
                          writeException[0] = e;
                        }
                      }
                    });

            dataWritingThread.start();
            Integer rows =
                stmt.executeUpdate(
                    String.format(
                        "LOAD DATA LOCAL INFILE '###.tsv' INTO TABLE %s",
                        SingleStoreUtil.escapeIdentifier(table)));

            context.output(rows);
            dataWritingThread.join();

            if (writeException[0] != null) {
              throw writeException[0];
            }
          } finally {
            stmt.close();
          }
        } finally {
          conn.close();
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      DataSourceConfiguration.populateDisplayData(getDataSourceConfiguration(), builder);
      builder.addIfNotNull(DisplayData.item("table", getTable()));
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(
          DisplayData.item(
              "userDataMapper", SingleStoreUtil.getClassNameOrNull(getUserDataMapper())));
    }
  }
}
