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
package org.apache.beam.sdk.io.snowflake;

import static org.apache.beam.sdk.io.TextIO.readFiles;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.snowflake.credentials.KeyPairSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on Snowflake.
 *
 * <p>SnowflakeIO uses <a href="https://docs.snowflake.net/manuals/user-guide/jdbc.html">Snowflake
 * JDBC</a> driver under the hood, but data isn't read/written using JDBC directly. Instead,
 * SnowflakeIO uses dedicated <b>COPY</b> operations to read/write data from/to a cloud bucket. By
 * now only Google Cloud Storage is supported.
 *
 * <p>To configure SnowflakeIO to read/write from your Snowflake instance, you have to provide a
 * {@link DataSourceConfiguration} using {@link
 * DataSourceConfiguration#create(SnowflakeCredentials)}, where {@link SnowflakeCredentials might be
 * created using {@link org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory}}.
 * Additionally one of {@link DataSourceConfiguration#withServerName(String)} or {@link
 * DataSourceConfiguration#withUrl(String)} must be used to tell SnowflakeIO which instance to use.
 * <br>
 * There are also other options available to configure connection to Snowflake:
 *
 * <ul>
 *   <li>{@link DataSourceConfiguration#withWarehouse(String)} to specify which Warehouse to use
 *   <li>{@link DataSourceConfiguration#withDatabase(String)} to specify which Database to connect
 *       to
 *   <li>{@link DataSourceConfiguration#withSchema(String)} to specify which schema to use
 *   <li>{@link DataSourceConfiguration#withRole(String)} to specify which role to use
 *   <li>{@link DataSourceConfiguration#withLoginTimeout(Integer)} to specify the timeout for the
 *       login
 *   <li>{@link DataSourceConfiguration#withPortNumber(Integer)} to specify custom port of Snowflake
 *       instance
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
 *     SnowflakeIO.DataSourceConfiguration.create(SnowflakeCredentialsFactory.of(options))
 *         .withServerName(options.getServerName())
 *         .withWarehouse(options.getWarehouse())
 *         .withDatabase(options.getDatabase())
 *         .withSchema(options.getSchema());
 * }</pre>
 *
 * <h3>Reading from Snowflake</h3>
 *
 * <p>SnowflakeIO.Read returns a bounded collection of {@code T} as a {@code PCollection<T>}. T is
 * the type returned by the provided {@link CsvMapper}.
 *
 * <p>For example
 *
 * <pre>{@code
 * PCollection<GenericRecord> items = pipeline.apply(
 *  SnowflakeIO.<GenericRecord>read()
 *    .withDataSourceConfiguration(dataSourceConfiguration)
 *    .fromQuery(QUERY)
 *    .withStagingBucketName(stagingBucketName)
 *    .withIntegrationName(integrationName)
 *    .withCsvMapper(...)
 *    .withCoder(...));
 * }</pre>
 *
 * <p><b>Important</b> When reading data from Snowflake, temporary CSV files are created on the
 * specified stagingBucketName in directory named `sf_copy_csv_[RANDOM CHARS]_[TIMESTAMP]`. This
 * directory and all the files are cleaned up automatically by default, but in case of failed pipeline
 * they may remain and will have to be cleaned up manually.
 */
public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  private static final String CSV_QUOTE_CHAR = "'";
  /**
   * Read data from Snowflake.
   *
   * @param snowflakeService user-defined {@link SnowflakeService}
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read(SnowflakeService snowflakeService) {
    return new AutoValue_SnowflakeIO_Read.Builder<T>()
        .setSnowflakeService(snowflakeService)
        .build();
  }

  /**
   * Read data from Snowflake.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return read(new SnowflakeServiceImpl());
  }

  /**
   * Interface for user-defined function mapping parts of CSV line into T. Used for
   * SnowflakeIO.Read.
   *
   * @param <T> Type of data to be read.
   */
  @FunctionalInterface
  public interface CsvMapper<T> extends Serializable {
    T mapRow(String[] parts) throws Exception;
  }

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract String getQuery();

    @Nullable
    abstract String getTable();

    @Nullable
    abstract String getIntegrationName();

    @Nullable
    abstract String getStagingBucketName();

    @Nullable
    abstract CsvMapper<T> getCsvMapper();

    @Nullable
    abstract Coder<T> getCoder();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(String query);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setIntegrationName(String integrationName);

      abstract Builder<T> setStagingBucketName(String stagingBucketName);

      abstract Builder<T> setCsvMapper(CsvMapper<T> csvMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Read<T> build();
    }

    /**
     * Setting information about Snowflake server.
     *
     * @param config - An instance of {@link DataSourceConfiguration}.
     */
    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {

      try {
        Connection connection = config.buildDatasource().getConnection();
        connection.close();
      } catch (SQLException e) {
        throw new IllegalArgumentException(
            "Invalid DataSourceConfiguration. Underlying cause: " + e);
      }
      return withDataSourceProviderFn(new DataSourceProviderFromDataSourceConfiguration(config));
    }

    /**
     * Setting function that will provide {@link DataSourceConfiguration} in runtime.
     *
     * @param dataSourceProviderFn a {@link SerializableFunction}.
     */
    public Read<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    /**
     * A query to be executed in Snowflake.
     *
     * @param query - String with query.
     */
    public Read<T> fromQuery(String query) {
      return toBuilder().setQuery(query).build();
    }

    /**
     * A table name to be read in Snowflake.
     *
     * @param table - String with the name of the table.
     */
    public Read<T> fromTable(String table) {
      return toBuilder().setTable(table).build();
    }

    /**
     * Name of the cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
     *
     * @param stagingBucketName - String with the name of the bucket.
     */
    public Read<T> withStagingBucketName(String stagingBucketName) {
      return toBuilder().setStagingBucketName(stagingBucketName).build();
    }

    /**
     * Name of the Storage Integration in Snowflake to be used. See
     * https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html for
     * reference.
     *
     * @param integrationName - String with the name of the Storage Integration.
     */
    public Read<T> withIntegrationName(String integrationName) {
      return toBuilder().setIntegrationName(integrationName).build();
    }

    /**
     * User-defined function mapping CSV lines into user data.
     *
     * @param csvMapper - an instance of {@link CsvMapper}.
     */
    public Read<T> withCsvMapper(CsvMapper<T> csvMapper) {
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    /**
     * A Coder to be used by the output PCollection generated by the source.
     *
     * @param coder - an instance of {@link Coder}.
     */
    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      // Either table or query is required. If query is present, it's being used, table is used
      // otherwise
      checkArgument(
          getQuery() != null || getTable() != null, "fromTable() or fromQuery() is required");
      checkArgument(
          !(getQuery() != null && getTable() != null),
          "fromTable() and fromQuery() are not allowed together");
      checkArgument(getCsvMapper() != null, "withCsvMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(getIntegrationName() != null, "withIntegrationName() is required");
      checkArgument(getStagingBucketName() != null, "withStagingBucketName() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      String gcpTmpDirName = makeTmpDirName();
      String stagingBucketDir = String.format("%s/%s/", getStagingBucketName(), gcpTmpDirName);

      PCollection<Void> emptyCollection = input.apply(Create.of((Void) null));

      PCollection<T> output =
          emptyCollection
              .apply(
                  ParDo.of(
                      new CopyIntoStageFn(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getTable(),
                          getIntegrationName(),
                          stagingBucketDir,
                          getSnowflakeService())))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply(readFiles())
              .apply(ParDo.of(new MapCsvToStringArrayFn()))
              .apply(ParDo.of(new MapStringArrayToUserDataFn<>(getCsvMapper())));

      output.setCoder(getCoder());

      emptyCollection
          .apply(Wait.on(output))
          .apply(ParDo.of(new CleanTmpFilesFromGcsFn(stagingBucketDir)));

      return output;
    }

    private String makeTmpDirName() {
      return String.format(
          "sf_copy_csv_%s_%s",
          new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()),
          UUID.randomUUID().toString().subSequence(0, 8) // first 8 chars of UUID should be enough
          );
    }

    private static class CopyIntoStageFn extends DoFn<Object, String> {
      private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
      private final String query;
      private final String table;
      private final String integrationName;
      private final String stagingBucketDir;
      private final SnowflakeService snowflakeService;

      private CopyIntoStageFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn,
          String query,
          String table,
          String integrationName,
          String stagingBucketDir,
          SnowflakeService snowflakeService) {
        this.dataSourceProviderFn = dataSourceProviderFn;
        this.query = query;
        this.table = table;
        this.integrationName = integrationName;
        this.stagingBucketDir = stagingBucketDir;
        this.snowflakeService = snowflakeService;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String output =
            snowflakeService.copyIntoStage(
                dataSourceProviderFn, query, table, integrationName, stagingBucketDir);

        context.output(output);
      }
    }

    public static class MapCsvToStringArrayFn extends DoFn<String, String[]> {
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String csvLine = c.element();
        CSVParser parser = new CSVParserBuilder().withQuoteChar(CSV_QUOTE_CHAR.charAt(0)).build();
        String[] parts = parser.parseLine(csvLine);
        c.output(parts);
      }
    }

    private static class MapStringArrayToUserDataFn<T> extends DoFn<String[], T> {
      private final CsvMapper<T> csvMapper;

      public MapStringArrayToUserDataFn(CsvMapper<T> csvMapper) {
        this.csvMapper = csvMapper;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        context.output(csvMapper.mapRow(context.element()));
      }
    }

    public static class CleanTmpFilesFromGcsFn extends DoFn<Object, Object> {
      private final String stagingBucketDir;

      public CleanTmpFilesFromGcsFn(String stagingBucketDir) {
        this.stagingBucketDir = stagingBucketDir;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String combinedPath = stagingBucketDir + "*";
        List<ResourceId> paths =
            FileSystems.match(combinedPath).metadata().stream()
                .map(metadata -> metadata.resourceId())
                .collect(Collectors.toList());

        FileSystems.delete(paths);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (getQuery() != null) {
        builder.add(DisplayData.item("query", getQuery()));
      }
      if (getTable() != null) {
        builder.add(DisplayData.item("table", getTable()));
      }
      builder.add(DisplayData.item("integrationName", getIntegrationName()));
      builder.add(DisplayData.item("stagingBucketName", getStagingBucketName()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /**
   * A POJO describing a {@link DataSource}, providing all properties allowing to create a {@link
   * DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {
    @Nullable
    public abstract String getUrl();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract PrivateKey getPrivateKey();

    @Nullable
    public abstract String getOauthToken();

    @Nullable
    public abstract String getDatabase();

    @Nullable
    public abstract String getWarehouse();

    @Nullable
    public abstract String getSchema();

    @Nullable
    public abstract String getServerName();

    @Nullable
    public abstract Integer getPortNumber();

    @Nullable
    public abstract String getRole();

    @Nullable
    public abstract Integer getLoginTimeout();

    @Nullable
    public abstract Boolean getSsl();

    @Nullable
    public abstract DataSource getDataSource();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUrl(String url);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setPrivateKey(PrivateKey privateKey);

      abstract Builder setOauthToken(String oauthToken);

      abstract Builder setDatabase(String database);

      abstract Builder setWarehouse(String warehouse);

      abstract Builder setSchema(String schema);

      abstract Builder setServerName(String serverName);

      abstract Builder setPortNumber(Integer portNumber);

      abstract Builder setRole(String role);

      abstract Builder setLoginTimeout(Integer loginTimeout);

      abstract Builder setSsl(Boolean ssl);

      abstract Builder setDataSource(DataSource dataSource);

      abstract DataSourceConfiguration build();
    }

    /**
     * Creates {@link DataSourceConfiguration} from existing instance of {@link DataSource}.
     *
     * @param dataSource - an instance of {@link DataSource}.
     */
    public static DataSourceConfiguration create(DataSource dataSource) {
      checkArgument(dataSource instanceof Serializable, "dataSource must be Serializable");
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
          .setDataSource(dataSource)
          .build();
    }

    /**
     * Creates {@link DataSourceConfiguration} from instance of {@link SnowflakeCredentials}.
     *
     * @param credentials - an instance of {@link SnowflakeCredentials}.
     */
    public static DataSourceConfiguration create(SnowflakeCredentials credentials) {
      if (credentials instanceof UsernamePasswordSnowflakeCredentials) {
        return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
            .setUsername(((UsernamePasswordSnowflakeCredentials) credentials).getUsername())
            .setPassword(((UsernamePasswordSnowflakeCredentials) credentials).getPassword())
            .build();
      } else if (credentials instanceof OAuthTokenSnowflakeCredentials) {
        return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
            .setOauthToken(((OAuthTokenSnowflakeCredentials) credentials).getToken())
            .build();
      } else if (credentials instanceof KeyPairSnowflakeCredentials) {
        return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder()
            .setUsername(((KeyPairSnowflakeCredentials) credentials).getUsername())
            .setPrivateKey(((KeyPairSnowflakeCredentials) credentials).getPrivateKey())
            .build();
      }
      throw new IllegalArgumentException(
          "Can't create DataSourceConfiguration from given credentials");
    }

    /**
     * Sets URL of Snowflake server in following format:
     * jdbc:snowflake://<account_name>.snowflakecomputing.com
     *
     * <p>Either withUrl or withServerName is required.
     *
     * @param url - String with URL of the Snowflake server.
     */
    public DataSourceConfiguration withUrl(String url) {
      checkArgument(
          url.startsWith("jdbc:snowflake://"),
          "url must have format: jdbc:snowflake://<account_name>.snowflakecomputing.com");
      checkArgument(
          url.endsWith("snowflakecomputing.com"),
          "url must have format: jdbc:snowflake://<account_name>.snowflakecomputing.com");
      return builder().setUrl(url).build();
    }

    /**
     * Sets database to use.
     *
     * @param database - String with database name.
     */
    public DataSourceConfiguration withDatabase(String database) {
      return builder().setDatabase(database).build();
    }

    /**
     * Sets Snowflake Warehouse to use.
     *
     * @param warehouse - String with warehouse name.
     */
    public DataSourceConfiguration withWarehouse(String warehouse) {
      return builder().setWarehouse(warehouse).build();
    }

    /**
     * Sets schema to use when connecting to Snowflake.
     *
     * @param schema - String with schema name.
     */
    public DataSourceConfiguration withSchema(String schema) {
      return builder().setSchema(schema).build();
    }

    /**
     * Sets the name of the Snowflake server. Following format is required:
     * <account_name>.snowflakecomputing.com
     *
     * <p>Either withServerName or withUrl is required.
     *
     * @param serverName - String with server name.
     */
    public DataSourceConfiguration withServerName(String serverName) {
      checkArgument(
          serverName.endsWith("snowflakecomputing.com"),
          "serverName must be in format <account_name>.snowflakecomputing.com");
      return builder().setServerName(serverName).build();
    }

    /**
     * Sets port number to use to connect to Snowflake.
     *
     * @param portNumber - Integer with port number.
     */
    public DataSourceConfiguration withPortNumber(Integer portNumber) {
      return builder().setPortNumber(portNumber).build();
    }

    /**
     * Sets user's role to be used when running queries on Snowflake.
     *
     * @param role - String with role name.
     */
    public DataSourceConfiguration withRole(String role) {
      return builder().setRole(role).build();
    }

    /**
     * Sets loginTimeout that will be used in {@link SnowflakeBasicDataSource:setLoginTimeout}.
     *
     * @param loginTimeout - Integer with timeout value.
     */
    public DataSourceConfiguration withLoginTimeout(Integer loginTimeout) {
      return builder().setLoginTimeout(loginTimeout).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      if (getDataSource() != null) {
        builder.addIfNotNull(DisplayData.item("dataSource", getDataSource().getClass().getName()));
      } else {
        builder.addIfNotNull(DisplayData.item("jdbcUrl", getUrl()));
        builder.addIfNotNull(DisplayData.item("username", getUsername()));
      }
    }

    /** Builds {@link SnowflakeBasicDataSource} based on the current configuration. */
    public DataSource buildDatasource() {
      if (getDataSource() == null) {
        SnowflakeBasicDataSource basicDataSource = new SnowflakeBasicDataSource();

        if (getUrl() != null) {
          basicDataSource.setUrl(getUrl());
        }
        if (getUsername() != null) {
          basicDataSource.setUser(getUsername());
        }
        if (getPassword() != null) {
          basicDataSource.setPassword(getPassword());
        }
        if (getPrivateKey() != null) {
          basicDataSource.setPrivateKey(getPrivateKey());
        }
        if (getDatabase() != null) {
          basicDataSource.setDatabaseName(getDatabase());
        }
        if (getWarehouse() != null) {
          basicDataSource.setWarehouse(getWarehouse());
        }
        if (getSchema() != null) {
          basicDataSource.setSchema(getSchema());
        }
        if (getServerName() != null) {
          basicDataSource.setServerName(getServerName());
        }
        if (getPortNumber() != null) {
          basicDataSource.setPortNumber(getPortNumber());
        }
        if (getRole() != null) {
          basicDataSource.setRole(getRole());
        }
        if (getLoginTimeout() != null) {
          try {
            basicDataSource.setLoginTimeout(getLoginTimeout());
          } catch (SQLException e) {
            throw new RuntimeException("Failed to setLoginTimeout");
          }
        }
        if (getOauthToken() != null) {
          basicDataSource.setOauthToken(getOauthToken());
        }
        return basicDataSource;
      }
      return getDataSource();
    }
  }

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
      return instances.computeIfAbsent(config, (config) -> config.buildDatasource());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }
}
