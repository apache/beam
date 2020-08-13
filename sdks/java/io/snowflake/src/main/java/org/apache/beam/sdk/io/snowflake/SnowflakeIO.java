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
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryResponse;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.StreamingLogLevel;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeBatchServiceConfig;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeBatchServiceImpl;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeService;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeStreamingServiceConfig;
import org.apache.beam.sdk.io.snowflake.services.SnowflakeStreamingServiceImpl;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.joda.time.Duration;
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
 * {@link DataSourceConfiguration} using {@link DataSourceConfiguration#create()}. Additionally one
 * of {@link DataSourceConfiguration#withServerName(String)} or {@link
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
 *     SnowflakeIO.DataSourceConfiguration.create()
 *         .withUsernamePasswordAuth(username, password)
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
 *    .withStagingBucketName(...)
 *    .withStorageIntegrationName(...)
 *    .withCsvMapper(...)
 *    .withCoder(...));
 * }</pre>
 *
 * <p><b>Important</b> When reading data from Snowflake, temporary CSV files are created on the
 * specified stagingBucketName in directory named `sf_copy_csv_[RANDOM CHARS]_[TIMESTAMP]`. This
 * directory and all the files are cleaned up automatically by default, but in case of failed
 * pipeline they may remain and will have to be cleaned up manually.
 *
 * <h3>Writing to Snowflake</h3>
 *
 * <p>SnowflakeIO.Write supports writing records into a database. It writes a {@link PCollection} to
 * the database by converting each T into a {@link Object[]} via a user-provided {@link
 * UserDataMapper}.
 *
 * <p>For example
 *
 * <pre>{@code
 * items.apply(
 *     SnowflakeIO.<KV<Integer, String>>write()
 *         .withDataSourceConfiguration(dataSourceConfiguration)
 *         .withStagingBucketName(...)
 *         .withStorageIntegrationName(...)
 *         .withUserDataMapper(maper)
 *         .to(table);
 * }</pre>
 *
 * <p><b>Important</b> When writing data to Snowflake, firstly data will be saved as CSV files on
 * specified stagingBucketName in directory named 'data' and then into Snowflake.
 */
@Experimental
public class SnowflakeIO {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIO.class);

  private static final String CSV_QUOTE_CHAR = "'";

  static final int DEFAULT_FLUSH_ROW_LIMIT = 10000;
  static final int DEFAULT_STREAMING_SHARDS_NUMBER = 1;
  static final int DEFAULT_BATCH_SHARDS_NUMBER = 0;
  static final Duration DEFAULT_FLUSH_TIME_LIMIT = Duration.millis(30000); // 30 seconds
  static final Duration DEFAULT_STREAMING_LOGS_MAX_SLEEP = Duration.standardMinutes(2);
  static final Duration DEFAULT_SLEEP_STREAMING_LOGS = Duration.standardSeconds(5000);

  /**
   * Read data from Snowflake via COPY statement using user-defined {@link SnowflakeService}.
   *
   * @param snowflakeService user-defined {@link SnowflakeService}
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read(SnowflakeService snowflakeService) {
    return new AutoValue_SnowflakeIO_Read.Builder<T>()
        .setSnowflakeService(snowflakeService)
        .setQuotationMark(CSV_QUOTE_CHAR)
        .build();
  }

  /**
   * Read data from Snowflake via COPY statement using default {@link SnowflakeBatchServiceImpl}.
   *
   * @param <T> Type of the data to be read.
   */
  public static <T> Read<T> read() {
    return read(new SnowflakeBatchServiceImpl());
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

  /**
   * Interface for user-defined function mapping T into array of Objects. Used for
   * SnowflakeIO.Write.
   *
   * @param <T> Type of data to be written.
   */
  @FunctionalInterface
  public interface UserDataMapper<T> extends Serializable {
    Object[] mapRow(T element);
  }

  /**
   * Write data to Snowflake via COPY statement.
   *
   * @param <T> Type of data to be written.
   */
  public static <T> Write<T> write() {
    return new AutoValue_SnowflakeIO_Write.Builder<T>()
        .setFileNameTemplate("output")
        .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .setWriteDisposition(WriteDisposition.APPEND)
        .setFlushTimeLimit(DEFAULT_FLUSH_TIME_LIMIT)
        .setShardsNumber(DEFAULT_BATCH_SHARDS_NUMBER)
        .setFlushRowLimit(DEFAULT_FLUSH_ROW_LIMIT)
        .setQuotationMark(CSV_QUOTE_CHAR)
        .build();
  }

  /** Implementation of {@link #read()}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getQuery();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getStorageIntegrationName();

    @Nullable
    abstract ValueProvider<String> getStagingBucketName();

    abstract @Nullable CsvMapper<T> getCsvMapper();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable SnowflakeService getSnowflakeService();

    @Nullable
    abstract String getQuotationMark();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setStorageIntegrationName(ValueProvider<String> storageIntegrationName);

      abstract Builder<T> setStagingBucketName(ValueProvider<String> stagingBucketName);

      abstract Builder<T> setCsvMapper(CsvMapper<T> csvMapper);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Builder<T> setQuotationMark(String quotationMark);

      abstract Read<T> build();
    }

    /**
     * Setting information about Snowflake server.
     *
     * @param config An instance of {@link DataSourceConfiguration}.
     */
    public Read<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(DataSourceProviderFromDataSourceConfiguration.of(config));
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
     * @param query String with query.
     */
    public Read<T> fromQuery(String query) {
      return toBuilder().setQuery(ValueProvider.StaticValueProvider.of(query)).build();
    }

    public Read<T> fromQuery(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    /**
     * A table name to be read in Snowflake.
     *
     * @param table String with the name of the table.
     */
    public Read<T> fromTable(String table) {
      return toBuilder().setTable(ValueProvider.StaticValueProvider.of(table)).build();
    }

    public Read<T> fromTable(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    /**
     * Name of the cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
     *
     * @param stagingBucketName String with the name of the bucket.
     */
    public Read<T> withStagingBucketName(String stagingBucketName) {
      checkArgument(
          stagingBucketName.endsWith("/"),
          "stagingBucketName must be a cloud storage path ending with /");
      return toBuilder()
          .setStagingBucketName(ValueProvider.StaticValueProvider.of(stagingBucketName))
          .build();
    }

    public Read<T> withStagingBucketName(ValueProvider<String> stagingBucketName) {
      return toBuilder().setStagingBucketName(stagingBucketName).build();
    }

    /**
     * Name of the Storage Integration in Snowflake to be used. See
     * https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html for
     * reference.
     *
     * @param integrationName String with the name of the Storage Integration.
     */
    public Read<T> withStorageIntegrationName(String integrationName) {
      return toBuilder()
          .setStorageIntegrationName(ValueProvider.StaticValueProvider.of(integrationName))
          .build();
    }

    public Read<T> withStorageIntegrationName(ValueProvider<String> integrationName) {
      return toBuilder().setStorageIntegrationName(integrationName).build();
    }

    /**
     * User-defined function mapping CSV lines into user data.
     *
     * @param csvMapper an instance of {@link CsvMapper}.
     */
    public Read<T> withCsvMapper(CsvMapper<T> csvMapper) {
      return toBuilder().setCsvMapper(csvMapper).build();
    }

    /**
     * A Coder to be used by the output PCollection generated by the source.
     *
     * @param coder an instance of {@link Coder}.
     */
    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /**
     * Sets Snowflake-specific quotations around strings.
     *
     * @param quotationMark with possible single quote {@code '}, double quote {@code "} or nothing.
     *     Default value is single quotation {@code '}.
     * @return
     */
    public Read<T> withQuotationMark(String quotationMark) {
      return toBuilder().setQuotationMark(quotationMark).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArguments();

      PCollection<Void> emptyCollection = input.apply(Create.of((Void) null));
      String tmpDirName = makeTmpDirName();
      PCollection<T> output =
          emptyCollection
              .apply(
                  ParDo.of(
                      new CopyIntoStageFn(
                          getDataSourceProviderFn(),
                          getQuery(),
                          getTable(),
                          getStorageIntegrationName(),
                          getStagingBucketName(),
                          tmpDirName,
                          getSnowflakeService(),
                          getQuotationMark())))
              .apply(Reshuffle.viaRandomKey())
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches())
              .apply(readFiles())
              .apply(ParDo.of(new MapCsvToStringArrayFn(getQuotationMark())))
              .apply(ParDo.of(new MapStringArrayToUserDataFn<>(getCsvMapper())));

      output.setCoder(getCoder());

      emptyCollection
          .apply(Wait.on(output))
          .apply(ParDo.of(new CleanTmpFilesFromGcsFn(getStagingBucketName(), tmpDirName)));
      return output;
    }

    private void checkArguments() {
      // Either table or query is required. If query is present, it's being used, table is used
      // otherwise

      checkArgument(
          getStorageIntegrationName() != null, "withStorageIntegrationName() is required");
      checkArgument(getStagingBucketName() != null, "withStagingBucketName() is required");

      checkArgument(
          getQuery() != null || getTable() != null, "fromTable() or fromQuery() is required");
      checkArgument(
          !(getQuery() != null && getTable() != null),
          "fromTable() and fromQuery() are not allowed together");
      checkArgument(getCsvMapper() != null, "withCsvMapper() is required");
      checkArgument(getCoder() != null, "withCoder() is required");
      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");
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
      private final ValueProvider<String> query;
      private final ValueProvider<String> database;
      private final ValueProvider<String> schema;
      private final ValueProvider<String> table;
      private final ValueProvider<String> storageIntegrationName;
      private final ValueProvider<String> stagingBucketDir;
      private final String tmpDirName;
      private final SnowflakeService snowflakeService;
      private final String quotationMark;

      private CopyIntoStageFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn,
          ValueProvider<String> query,
          ValueProvider<String> table,
          ValueProvider<String> storageIntegrationName,
          ValueProvider<String> stagingBucketDir,
          String tmpDirName,
          SnowflakeService snowflakeService,
          String quotationMark) {
        this.dataSourceProviderFn = dataSourceProviderFn;
        this.query = query;
        this.table = table;
        this.storageIntegrationName = storageIntegrationName;
        this.snowflakeService = snowflakeService;
        this.quotationMark = quotationMark;
        this.stagingBucketDir = stagingBucketDir;
        this.tmpDirName = tmpDirName;
        DataSourceProviderFromDataSourceConfiguration
            dataSourceProviderFromDataSourceConfiguration =
                (DataSourceProviderFromDataSourceConfiguration) this.dataSourceProviderFn;
        DataSourceConfiguration config = dataSourceProviderFromDataSourceConfiguration.getConfig();

        this.database = config.getDatabase();
        this.schema = config.getSchema();
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        String databaseValue = getValueOrNull(this.database);
        String schemaValue = getValueOrNull(this.schema);
        String tableValue = getValueOrNull(this.table);
        String queryValue = getValueOrNull(this.query);

        String stagingBucketRunDir =
            String.format(
                "%s/%s/run_%s/",
                stagingBucketDir.get(), tmpDirName, UUID.randomUUID().toString().subSequence(0, 8));

        SnowflakeBatchServiceConfig config =
            new SnowflakeBatchServiceConfig(
                dataSourceProviderFn,
                databaseValue,
                schemaValue,
                tableValue,
                queryValue,
                storageIntegrationName.get(),
                stagingBucketRunDir,
                quotationMark);

        String output = snowflakeService.read(config);

        context.output(output);
      }
    }

    /**
     * Parses {@code String} from incoming data in {@link PCollection} to have proper format for CSV
     * files.
     */
    public static class MapCsvToStringArrayFn extends DoFn<String, String[]> {
      private String quoteChar;

      public MapCsvToStringArrayFn(String quoteChar) {
        this.quoteChar = quoteChar;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String csvLine = c.element();
        CSVParser parser = new CSVParserBuilder().withQuoteChar(quoteChar.charAt(0)).build();
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

    /** Removes temporary staged files after reading. */
    public static class CleanTmpFilesFromGcsFn extends DoFn<Object, Object> {
      private final ValueProvider<String> stagingBucketDir;
      private final String tmpDirName;

      /**
       * Created object that will remove temp files from stage.
       *
       * @param stagingBucketDir bucket and directory where temporary files are saved
       * @param tmpDirName temporary directory created on bucket where files were saved
       */
      public CleanTmpFilesFromGcsFn(ValueProvider<String> stagingBucketDir, String tmpDirName) {
        this.stagingBucketDir = stagingBucketDir;
        this.tmpDirName = tmpDirName;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        String combinedPath = String.format("%s/%s/**", stagingBucketDir.get(), tmpDirName);
        List<ResourceId> paths =
            FileSystems.match(combinedPath).metadata().stream()
                .map(metadata -> metadata.resourceId())
                .collect(Collectors.toList());

        FileSystems.delete(paths, MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
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
      builder.add(DisplayData.item("storageIntegrationName", getStagingBucketName()));
      builder.add(DisplayData.item("stagingBucketName", getStagingBucketName()));
      builder.add(DisplayData.item("csvMapper", getCsvMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDataSourceProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
      }
    }
  }

  /** Implementation of {@link #write()}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

    @Nullable
    abstract ValueProvider<String> getTable();

    @Nullable
    abstract ValueProvider<String> getStorageIntegrationName();

    @Nullable
    abstract ValueProvider<String> getStagingBucketName();

    @Nullable
    abstract ValueProvider<String> getQuery();

    @Nullable
    abstract ValueProvider<String> getSnowPipe();

    @Nullable
    abstract Integer getFlushRowLimit();

    @Nullable
    abstract Integer getShardsNumber();

    @Nullable
    abstract Duration getFlushTimeLimit();

    @Nullable
    abstract String getFileNameTemplate();

    @Nullable
    abstract WriteDisposition getWriteDisposition();

    @Nullable
    abstract CreateDisposition getCreateDisposition();

    @Nullable
    abstract UserDataMapper getUserDataMapper();

    @Nullable
    abstract SnowflakeTableSchema getTableSchema();

    @Nullable
    abstract SnowflakeService getSnowflakeService();

    @Nullable
    abstract String getQuotationMark();

    @Nullable
    abstract StreamingLogLevel getDebugMode();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setDataSourceProviderFn(
          SerializableFunction<Void, DataSource> dataSourceProviderFn);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setQuery(ValueProvider<String> query);

      abstract Builder<T> setStorageIntegrationName(ValueProvider<String> storageIntegrationName);

      abstract Builder<T> setStagingBucketName(ValueProvider<String> stagingBucketName);

      abstract Builder<T> setSnowPipe(ValueProvider<String> snowPipe);

      abstract Builder<T> setFlushRowLimit(Integer rowsCount);

      abstract Builder<T> setShardsNumber(Integer shardsNumber);

      abstract Builder<T> setFlushTimeLimit(Duration triggeringFrequency);

      abstract Builder<T> setFileNameTemplate(String fileNameTemplate);

      abstract Builder<T> setUserDataMapper(UserDataMapper userDataMapper);

      abstract Builder<T> setWriteDisposition(WriteDisposition writeDisposition);

      abstract Builder<T> setCreateDisposition(CreateDisposition createDisposition);

      abstract Builder<T> setTableSchema(SnowflakeTableSchema tableSchema);

      abstract Builder<T> setSnowflakeService(SnowflakeService snowflakeService);

      abstract Builder<T> setQuotationMark(String quotationMark);

      abstract Builder<T> setDebugMode(StreamingLogLevel debugLevel);

      abstract Write<T> build();
    }

    /**
     * Setting information about Snowflake server.
     *
     * @param config An instance of {@link DataSourceConfiguration}.
     */
    public Write<T> withDataSourceConfiguration(final DataSourceConfiguration config) {
      return withDataSourceProviderFn(DataSourceProviderFromDataSourceConfiguration.of(config));
    }

    /**
     * Setting function that will provide {@link DataSourceConfiguration} in runtime.
     *
     * @param dataSourceProviderFn a {@link SerializableFunction}.
     */
    public Write<T> withDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn) {
      return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
    }

    /**
     * A table name to be written in Snowflake.
     *
     * @param table String with the name of the table.
     */
    public Write<T> to(String table) {
      return toBuilder().setTable(ValueProvider.StaticValueProvider.of(table)).build();
    }

    public Write<T> to(ValueProvider<String> table) {
      return toBuilder().setTable(table).build();
    }

    /**
     * Name of the cloud bucket (GCS by now) to use as tmp location of CSVs during COPY statement.
     *
     * @param stagingBucketName String with the name of the bucket.
     */
    public Write<T> withStagingBucketName(String stagingBucketName) {
      checkArgument(
          stagingBucketName.endsWith("/"),
          "stagingBucketName must be a cloud storage path ending with /");
      return toBuilder()
          .setStagingBucketName(ValueProvider.StaticValueProvider.of(stagingBucketName))
          .build();
    }

    public Write<T> withStagingBucketName(ValueProvider<String> stagingBucketName) {
      return toBuilder().setStagingBucketName(stagingBucketName).build();
    }

    /**
     * Name of the Storage Integration in Snowflake to be used. See
     * https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html for
     * reference.
     *
     * @param integrationName String with the name of the Storage Integration.
     */
    public Write<T> withStorageIntegrationName(String integrationName) {
      return toBuilder()
          .setStorageIntegrationName(ValueProvider.StaticValueProvider.of(integrationName))
          .build();
    }

    public Write<T> withStorageIntegrationName(ValueProvider<String> integrationName) {
      return toBuilder().setStorageIntegrationName(integrationName).build();
    }

    /**
     * A query to be executed in Snowflake.
     *
     * @param query String with query.
     */
    public Write<T> withQueryTransformation(String query) {
      return toBuilder().setQuery(ValueProvider.StaticValueProvider.of(query)).build();
    }

    public Write<T> withQueryTransformation(ValueProvider<String> query) {
      return toBuilder().setQuery(query).build();
    }

    /**
     * A template name for files saved to GCP.
     *
     * @param fileNameTemplate String with template name for files.
     */
    public Write<T> withFileNameTemplate(String fileNameTemplate) {
      return toBuilder().setFileNameTemplate(fileNameTemplate).build();
    }

    /**
     * User-defined function mapping user data into CSV lines.
     *
     * @param userDataMapper an instance of {@link UserDataMapper}.
     */
    public Write<T> withUserDataMapper(UserDataMapper userDataMapper) {
      return toBuilder().setUserDataMapper(userDataMapper).build();
    }

    /**
     * Sets duration how often staged files will be created and then how often ingested by Snowflake
     * during streaming.
     *
     * @param triggeringFrequency time for triggering frequency in {@link Duration} type.
     * @return
     */
    public Write<T> withFlushTimeLimit(Duration triggeringFrequency) {
      return toBuilder().setFlushTimeLimit(triggeringFrequency).build();
    }

    /**
     * Sets name of <a
     * href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro.html">SnowPipe</a>
     * which can be created in Snowflake dashboard or cli:
     *
     * <pre>{@code
     * CREATE snowPipeName AS COPY INTO your_table from @yourstage;
     * }</pre>
     *
     * <p>The stage in <a
     * href="https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html">COPY</a>
     * statement should be pointing to the cloud <a
     * href="https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html">integration</a>
     * with the valid bucket url, ex. for GCS:
     *
     * <pre>{@code
     * CREATE STAGE yourstage
     * URL = 'gcs://yourbucket/path/'
     * STORAGE_INTEGRATION = your_integration;
     * }</pre>
     *
     * <pre>{@code
     * CREATE STORAGE INTEGRATION your_integration
     *   TYPE = EXTERNAL_STAGE
     *   STORAGE_PROVIDER = GCS
     *   ENABLED = TRUE
     *   STORAGE_ALLOWED_LOCATIONS = ('gcs://yourbucket/path/')
     * }</pre>
     *
     * @param snowPipe name of created SnowPipe in Snowflake dashboard.
     * @return
     */
    public Write<T> withSnowPipe(String snowPipe) {
      return toBuilder().setSnowPipe(ValueProvider.StaticValueProvider.of(snowPipe)).build();
    }

    /**
     * Same as {@code withSnowPipe(String}, but with a {@link ValueProvider}.
     *
     * @param snowPipe name of created SnowPipe in Snowflake dashboard.
     * @return
     */
    public Write<T> withSnowPipe(ValueProvider<String> snowPipe) {
      return toBuilder().setSnowPipe(snowPipe).build();
    }

    /**
     * Number of shards that are created per window.
     *
     * @param shardsNumber defined number of shards or 1 by default.
     * @return
     */
    public Write<T> withShardsNumber(Integer shardsNumber) {
      return toBuilder().setShardsNumber(shardsNumber).build();
    }

    /**
     * Sets number of row limit that will be saved to the staged file and then loaded to Snowflake.
     * If the number of rows will be lower than the limit it will be loaded with current number of
     * rows after certain time specified by setting {@code withFlushTimeLimit(Duration
     * triggeringFrequency)}
     *
     * @param rowsCount Number of rows that will be in one file staged for loading. Default: 10000.
     * @return
     */
    public Write<T> withFlushRowLimit(Integer rowsCount) {
      return toBuilder().setFlushRowLimit(rowsCount).build();
    }

    /**
     * A disposition to be used during writing to table phase.
     *
     * @param writeDisposition an instance of {@link WriteDisposition}.
     */
    public Write<T> withWriteDisposition(WriteDisposition writeDisposition) {
      return toBuilder().setWriteDisposition(writeDisposition).build();
    }

    /**
     * A disposition to be used during table preparation.
     *
     * @param createDisposition - an instance of {@link CreateDisposition}.
     */
    public Write<T> withCreateDisposition(CreateDisposition createDisposition) {
      return toBuilder().setCreateDisposition(createDisposition).build();
    }

    /**
     * Table schema to be used during creating table.
     *
     * @param tableSchema - an instance of {@link SnowflakeTableSchema}.
     */
    public Write<T> withTableSchema(SnowflakeTableSchema tableSchema) {
      return toBuilder().setTableSchema(tableSchema).build();
    }

    /**
     * A snowflake service {@link SnowflakeService} implementation which is supposed to be used.
     *
     * @param snowflakeService an instance of {@link SnowflakeService}.
     */
    public Write<T> withSnowflakeService(SnowflakeService snowflakeService) {
      return toBuilder().setSnowflakeService(snowflakeService).build();
    }

    /**
     * Sets Snowflake-specific quotations around strings.
     *
     * @param quotationMark with possible single quote {@code '}, double quote {@code "} or nothing.
     *     Default value is single quotation {@code '}.
     * @return
     */
    public Write<T> withQuotationMark(String quotationMark) {
      return toBuilder().setQuotationMark(quotationMark).build();
    }

    /**
     * The option to verbose info (or only errors) of loaded files while streaming. It is not set by
     * default because it may influence performance. For details: <a
     * href="https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html#endpoint-insertreport">insert
     * report REST API.</a>
     *
     * @param debugLevel error or info debug level from enum {@link StreamingLogLevel}
     * @return
     */
    public Write<T> withDebugMode(StreamingLogLevel debugLevel) {
      return toBuilder().setDebugMode(debugLevel).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArguments(input);

      PCollection out;

      if (getSnowPipe() != null) {
        out = writeStream(input, getStagingBucketName());
      } else {
        out = writeBatch(input, getStagingBucketName());
      }

      out.setCoder(StringUtf8Coder.of());

      return PDone.in(out.getPipeline());
    }

    private void checkArguments(PCollection<T> input) {
      checkArgument(getStagingBucketName() != null, "withStagingBucketName is required");

      checkArgument(getUserDataMapper() != null, "withUserDataMapper() is required");

      checkArgument(
          (getDataSourceProviderFn() != null),
          "withDataSourceConfiguration() or withDataSourceProviderFn() is required");

      if (input.isBounded() == PCollection.IsBounded.UNBOUNDED) {
        checkArgument(
            getSnowPipe() != null,
            "in streaming (unbounded) write it is required to specify SnowPipe name via withSnowPipe() method.");
      } else {
        checkArgument(
            getTable() != null,
            "in batch writing it is required to specify destination table name via to() method.");
      }
    }

    private PCollection<T> writeStream(
        PCollection<T> input, ValueProvider<String> stagingBucketDir) {
      SnowflakeService snowflakeService =
          getSnowflakeService() != null
              ? getSnowflakeService()
              : new SnowflakeStreamingServiceImpl();

      /* Ensure that files will be created after specific record count or duration specified */
      PCollection<T> inputInGlobalWindow =
          input.apply(
              "Rewindow Into Global",
              Window.<T>into(new GlobalWindows())
                  .triggering(
                      Repeatedly.forever(
                          AfterFirst.of(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(getFlushTimeLimit()),
                              AfterPane.elementCountAtLeast(getFlushRowLimit()))))
                  .discardingFiredPanes());

      int shards = (getShardsNumber() > 0) ? getShardsNumber() : DEFAULT_STREAMING_SHARDS_NUMBER;
      PCollection files = writeFiles(inputInGlobalWindow, stagingBucketDir, shards);

      /* Ensuring that files will be ingested after flush time */
      files =
          (PCollection)
              files.apply(
                  "Apply User Trigger",
                  Window.<T>into(new GlobalWindows())
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(getFlushTimeLimit())))
                      .discardingFiredPanes());
      files =
          (PCollection)
              files.apply(
                  "Create list of files for loading via SnowPipe",
                  Combine.globally(new Concatenate()).withoutDefaults());

      return (PCollection)
          files.apply("Stream files to table", streamToTable(snowflakeService, stagingBucketDir));
    }

    private PCollection writeBatch(PCollection input, ValueProvider<String> stagingBucketDir) {
      SnowflakeService snowflakeService =
          getSnowflakeService() != null ? getSnowflakeService() : new SnowflakeBatchServiceImpl();

      PCollection<String> files = writeBatchFiles(input, stagingBucketDir);

      // Combining PCollection of files as a side input into one list of files
      ListCoder<String> coder = ListCoder.of(StringUtf8Coder.of());
      files =
          (PCollection)
              files
                  .getPipeline()
                  .apply(
                      Reify.viewInGlobalWindow(
                          (PCollectionView) files.apply(View.asList()), coder));

      return (PCollection)
          files.apply("Copy files to table", copyToTable(snowflakeService, stagingBucketDir));
    }

    private PCollection writeBatchFiles(
        PCollection<T> input, ValueProvider<String> outputDirectory) {
      return writeFiles(input, outputDirectory, DEFAULT_BATCH_SHARDS_NUMBER);
    }

    private PCollection<String> writeFiles(
        PCollection<T> input, ValueProvider<String> stagingBucketDir, int numShards) {

      PCollection<String> mappedUserData =
          input
              .apply(
                  MapElements.via(
                      new SimpleFunction<T, Object[]>() {
                        @Override
                        public Object[] apply(T element) {
                          return getUserDataMapper().mapRow(element);
                        }
                      }))
              .apply(
                  "Map Objects array to CSV lines",
                  ParDo.of(new MapObjectsArrayToCsvFn(getQuotationMark())))
              .setCoder(StringUtf8Coder.of());

      WriteFilesResult filesResult =
          mappedUserData.apply(
              "Write files to specified location",
              FileIO.<String>write()
                  .via(TextIO.sink())
                  .to(stagingBucketDir)
                  .withPrefix(UUID.randomUUID().toString().subSequence(0, 8).toString())
                  .withSuffix(".csv")
                  .withNumShards(numShards)
                  .withCompression(Compression.GZIP));

      return (PCollection)
          filesResult
              .getPerDestinationOutputFilenames()
              .apply("Parse KV filenames to Strings", Values.<String>create());
    }

    private ParDo.SingleOutput<Object, Object> copyToTable(
        SnowflakeService snowflakeService, ValueProvider<String> stagingBucketDir) {
      return ParDo.of(
          new CopyToTableFn<>(
              getDataSourceProviderFn(),
              getTable(),
              getQuery(),
              stagingBucketDir,
              getStorageIntegrationName(),
              getCreateDisposition(),
              getWriteDisposition(),
              getTableSchema(),
              snowflakeService,
              getQuotationMark()));
    }

    protected PTransform streamToTable(
        SnowflakeService snowflakeService, ValueProvider<String> stagingBucketDir) {
      return ParDo.of(
          new StreamToTableFn(
              getDataSourceProviderFn(),
              getSnowPipe(),
              stagingBucketDir,
              getDebugMode(),
              snowflakeService));
    }
  }

  /**
   * Combines list of {@code String} to provide one {@code String} with paths where files were
   * staged for write.
   */
  public static class Concatenate extends Combine.CombineFn<String, List<String>, List<String>> {
    @Override
    public List<String> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<String> addInput(List<String> mutableAccumulator, String input) {
      mutableAccumulator.add(String.format("'%s'", input));
      return mutableAccumulator;
    }

    @Override
    public List<String> mergeAccumulators(Iterable<List<String>> accumulators) {
      List<String> result = createAccumulator();
      for (List<String> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<String> extractOutput(List<String> accumulator) {
      return accumulator;
    }
  }

  /**
   * Custom DoFn that maps {@link Object[]} into CSV line to be saved to Snowflake.
   *
   * <p>Adds Snowflake-specific quotations around strings.
   */
  private static class MapObjectsArrayToCsvFn extends DoFn<Object[], String> {
    private String quotationMark;

    public MapObjectsArrayToCsvFn(String quotationMark) {
      this.quotationMark = quotationMark;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      List<Object> csvItems = new ArrayList<>();
      for (Object o : context.element()) {
        if (o instanceof String) {
          String field = (String) o;
          field = field.replace("'", "''");
          field = quoteField(field);

          csvItems.add(field);
        } else {
          csvItems.add(o);
        }
      }
      context.output(Joiner.on(",").useForNull("").join(csvItems));
    }

    private String quoteField(String field) {
      return quoteField(field, this.quotationMark);
    }

    private String quoteField(String field, String quotation) {
      return String.format("%s%s%s", quotation, field, quotation);
    }
  }

  private static class CopyToTableFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> table;
    private final ValueProvider<String> database;
    private final ValueProvider<String> schema;
    private final ValueProvider<String> query;
    private final SnowflakeTableSchema tableSchema;
    private final String quotationMark;
    private final ValueProvider<String> stagingBucketDir;
    private final ValueProvider<String> storageIntegrationName;
    private final WriteDisposition writeDisposition;
    private final CreateDisposition createDisposition;
    private final SnowflakeService snowflakeService;

    CopyToTableFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> table,
        ValueProvider<String> query,
        ValueProvider<String> stagingBucketDir,
        ValueProvider<String> storageIntegrationName,
        CreateDisposition createDisposition,
        WriteDisposition writeDisposition,
        SnowflakeTableSchema tableSchema,
        SnowflakeService snowflakeService,
        String quotationMark) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.query = query;
      this.table = table;
      this.tableSchema = tableSchema;
      this.stagingBucketDir = stagingBucketDir;
      this.storageIntegrationName = storageIntegrationName;
      this.writeDisposition = writeDisposition;
      this.createDisposition = createDisposition;
      this.snowflakeService = snowflakeService;
      this.quotationMark = quotationMark;

      DataSourceProviderFromDataSourceConfiguration dataSourceProviderFromDataSourceConfiguration =
          (DataSourceProviderFromDataSourceConfiguration) this.dataSourceProviderFn;
      DataSourceConfiguration config = dataSourceProviderFromDataSourceConfiguration.getConfig();

      this.database = config.getDatabase();
      this.schema = config.getSchema();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      String databaseValue = getValueOrNull(this.database);
      String schemaValue = getValueOrNull(this.schema);
      String tableValue = getValueOrNull(this.table);
      String queryValue = getValueOrNull(this.query);

      SnowflakeBatchServiceConfig config =
          new SnowflakeBatchServiceConfig(
              dataSourceProviderFn,
              (List<String>) context.element(),
              tableSchema,
              databaseValue,
              schemaValue,
              tableValue,
              queryValue,
              createDisposition,
              writeDisposition,
              storageIntegrationName.get(),
              stagingBucketDir.get(),
              quotationMark);
      snowflakeService.write(config);
    }
  }

  /** Custom DoFn that streams data to Snowflake table. */
  private static class StreamToTableFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    private final SerializableFunction<Void, DataSource> dataSourceProviderFn;
    private final ValueProvider<String> stagingBucketDir;
    private final ValueProvider<String> snowPipe;
    private final StreamingLogLevel debugMode;
    private final SnowflakeService snowflakeService;
    private transient SimpleIngestManager ingestManager;

    private transient DataSource dataSource;
    ArrayList<String> trackedFilesNames;

    StreamToTableFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn,
        ValueProvider<String> snowPipe,
        ValueProvider<String> stagingBucketDir,
        StreamingLogLevel debugMode,
        SnowflakeService snowflakeService) {
      this.dataSourceProviderFn = dataSourceProviderFn;
      this.stagingBucketDir = stagingBucketDir;
      this.snowPipe = snowPipe;
      this.debugMode = debugMode;
      this.snowflakeService = snowflakeService;
      trackedFilesNames = new ArrayList<>();
    }

    @Setup
    public void setup() throws Exception {
      dataSource = dataSourceProviderFn.apply(null);

      DataSourceProviderFromDataSourceConfiguration dataSourceProviderFromDataSourceConfiguration =
          (DataSourceProviderFromDataSourceConfiguration) this.dataSourceProviderFn;
      DataSourceConfiguration config = dataSourceProviderFromDataSourceConfiguration.getConfig();

      PrivateKey privateKey = null;
      if (config.getPrivateKey() != null) {
        privateKey = config.getPrivateKey();
      } else if (isNotEmpty(config.getPrivateKeyPassphrase())
          && isNotEmpty(config.getRawPrivateKey())) {
        privateKey =
            KeyPairUtils.preparePrivateKey(
                config.getRawPrivateKey().get(), config.getPrivateKeyPassphrase().get());
      }

      checkArgument(privateKey != null, "KeyPair is required for authentication");

      String hostName = config.getServerName().get();
      List<String> path = Splitter.on('.').splitToList(hostName);
      String account = path.get(0);
      String username = config.getUsername().get();
      String schema = config.getSchema().get();
      String database = config.getDatabase().get();
      String snowPipeName = String.format("%s.%s.%s", database, schema, snowPipe.get());

      this.ingestManager =
          new SimpleIngestManager(
              account, username, snowPipeName, privateKey, "https", hostName, 443);
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      List<String> filesList = (List<String>) context.element();

      if (debugMode != null) {
        trackedFilesNames.addAll(filesList);
      }
      SnowflakeStreamingServiceConfig config =
          new SnowflakeStreamingServiceConfig(
              filesList, this.stagingBucketDir.get(), this.ingestManager);
      snowflakeService.write(config);
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (debugMode != null) {
        String beginMark = null;
        Duration currentSleep = Duration.ZERO;

        while (currentSleep.isShorterThan(DEFAULT_STREAMING_LOGS_MAX_SLEEP)
            && trackedFilesNames.size() > 0) {
          Thread.sleep(DEFAULT_SLEEP_STREAMING_LOGS.getMillis());
          currentSleep = currentSleep.plus(DEFAULT_SLEEP_STREAMING_LOGS);
          HistoryResponse response = ingestManager.getHistory(null, null, beginMark);

          if (response != null && response.getNextBeginMark() != null) {
            beginMark = response.getNextBeginMark();
          }
          if (response != null && response.files != null) {
            response.files.forEach(
                entry -> {
                  if (entry.getPath() != null && entry.isComplete()) {
                    String responseFileName =
                        String.format("'%s%s'", entry.getStageLocation(), entry.getPath())
                            .toLowerCase()
                            .replace("gcs://", "gs://");
                    if (trackedFilesNames.contains(responseFileName)) {
                      trackedFilesNames.remove(responseFileName);

                      if (entry.getErrorsSeen() > 0) {
                        LOG.error(String.format("Snowflake SnowPipe ERROR: %s", entry.toString()));
                      } else if (entry.getErrorsSeen() == 0
                          && debugMode.equals(StreamingLogLevel.INFO)) {
                        LOG.info(String.format("Snowflake SnowPipe INFO: %s", entry.toString()));
                      }
                    }
                  }
                });
          }
        }
        trackedFilesNames.forEach(
            file -> LOG.info(String.format("File %s was not found in ingest history", file)));
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
    public abstract ValueProvider<String> getUsername();

    @Nullable
    public abstract ValueProvider<String> getPassword();

    @Nullable
    public abstract PrivateKey getPrivateKey();

    @Nullable
    public abstract String getPrivateKeyPath();

    @Nullable
    public abstract ValueProvider<String> getRawPrivateKey();

    @Nullable
    public abstract ValueProvider<String> getPrivateKeyPassphrase();

    @Nullable
    public abstract ValueProvider<String> getOauthToken();

    @Nullable
    public abstract ValueProvider<String> getDatabase();

    @Nullable
    public abstract ValueProvider<String> getWarehouse();

    @Nullable
    public abstract ValueProvider<String> getSchema();

    @Nullable
    public abstract ValueProvider<String> getServerName();

    @Nullable
    public abstract Integer getPortNumber();

    @Nullable
    public abstract ValueProvider<String> getRole();

    @Nullable
    public abstract String getAuthenticator();

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

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setPrivateKey(PrivateKey privateKey);

      abstract Builder setPrivateKeyPath(String privateKeyPath);

      abstract Builder setRawPrivateKey(ValueProvider<String> rawPrivateKey);

      abstract Builder setPrivateKeyPassphrase(ValueProvider<String> privateKeyPassphrase);

      abstract Builder setOauthToken(ValueProvider<String> oauthToken);

      abstract Builder setDatabase(ValueProvider<String> database);

      abstract Builder setWarehouse(ValueProvider<String> warehouse);

      abstract Builder setSchema(ValueProvider<String> schema);

      abstract Builder setServerName(ValueProvider<String> serverName);

      abstract Builder setPortNumber(Integer portNumber);

      abstract Builder setRole(ValueProvider<String> role);

      abstract Builder setAuthenticator(String authenticator);

      abstract Builder setLoginTimeout(Integer loginTimeout);

      abstract Builder setSsl(Boolean ssl);

      abstract Builder setDataSource(DataSource dataSource);

      abstract DataSourceConfiguration build();
    }

    public static DataSourceConfiguration create() {
      return new AutoValue_SnowflakeIO_DataSourceConfiguration.Builder().build();
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
     * Sets username/password authentication.
     *
     * @param username - Snowflake username.
     * @param password - Password for provided Snowflake username.
     */
    public DataSourceConfiguration withUsernamePasswordAuth(String username, String password) {
      return builder()
          .setUsername(ValueProvider.StaticValueProvider.of(username))
          .setPassword(ValueProvider.StaticValueProvider.of(password))
          .build();
    }

    /**
     * Sets username/password authentication.
     *
     * @param username - Snowflake username.
     * @param password - Password for provided Snowflake username.
     */
    public DataSourceConfiguration withUsernamePasswordAuth(
        ValueProvider<String> username, ValueProvider<String> password) {
      return builder().setUsername(username).setPassword(password).build();
    }

    /**
     * Sets OAuth authentication.
     *
     * @param token - OAuth token.
     */
    public DataSourceConfiguration withOAuth(String token) {
      return builder().setOauthToken(ValueProvider.StaticValueProvider.of(token)).build();
    }

    /**
     * Sets OAuth authentication.
     *
     * @param token - OAuth token.
     */
    public DataSourceConfiguration withOAuth(ValueProvider<String> token) {
      return builder().setOauthToken(token).build();
    }

    /**
     * Sets key pair authentication.
     *
     * @param username - Snowflake username.
     * @param privateKey - Private key.
     */
    public DataSourceConfiguration withKeyPairAuth(String username, PrivateKey privateKey) {
      return builder()
          .setUsername(ValueProvider.StaticValueProvider.of(username))
          .setPrivateKey(privateKey)
          .build();
    }

    /**
     * Sets key pair authentication.
     *
     * @param username - Snowflake username.
     * @param privateKeyPath - Private key path.
     * @param privateKeyPassphrase - Passphrase for provided private key.
     */
    public DataSourceConfiguration withKeyPairPathAuth(
        ValueProvider<String> username,
        String privateKeyPath,
        ValueProvider<String> privateKeyPassphrase) {
      String privateKey = KeyPairUtils.readPrivateKeyFile(privateKeyPath);
      return builder()
          .setUsername(username)
          .setRawPrivateKey(ValueProvider.StaticValueProvider.of(privateKey))
          .setPrivateKeyPassphrase(privateKeyPassphrase)
          .build();
    }

    /**
     * Sets key pair authentication.
     *
     * @param username - Snowflake username.
     * @param privateKeyPath - Private key path.
     * @param privateKeyPassphrase - Passphrase for provided private key.
     */
    public DataSourceConfiguration withKeyPairPathAuth(
        String username, String privateKeyPath, String privateKeyPassphrase) {
      String privateKey = KeyPairUtils.readPrivateKeyFile(privateKeyPath);

      return builder()
          .setUsername(ValueProvider.StaticValueProvider.of(username))
          .setRawPrivateKey(ValueProvider.StaticValueProvider.of(privateKey))
          .setPrivateKeyPassphrase(ValueProvider.StaticValueProvider.of((privateKeyPassphrase)))
          .build();
    }

    /**
     * Sets key pair authentication.
     *
     * @param username - Snowflake username.
     * @param rawPrivateKey - Raw private key.
     * @param privateKeyPassphrase - Passphrase for provided private key.
     */
    public DataSourceConfiguration withKeyPairRawAuth(
        ValueProvider<String> username,
        ValueProvider<String> rawPrivateKey,
        ValueProvider<String> privateKeyPassphrase) {
      return builder()
          .setUsername(username)
          .setRawPrivateKey(rawPrivateKey)
          .setPrivateKeyPassphrase(privateKeyPassphrase)
          .build();
    }

    /**
     * Sets key pair authentication.
     *
     * @param username - Snowflake username.
     * @param rawPrivateKey - Raw private key.
     * @param privateKeyPassphrase - Passphrase for provided private key.
     */
    public DataSourceConfiguration withKeyPairRawAuth(
        String username, String rawPrivateKey, String privateKeyPassphrase) {
      return builder()
          .setUsername(ValueProvider.StaticValueProvider.of(username))
          .setRawPrivateKey(ValueProvider.StaticValueProvider.of((rawPrivateKey)))
          .setPrivateKeyPassphrase(ValueProvider.StaticValueProvider.of((privateKeyPassphrase)))
          .build();
    }

    /**
     * Sets URL of Snowflake server in following format:
     * jdbc:snowflake://<account_name>.snowflakecomputing.com
     *
     * <p>Either withUrl or withServerName is required.
     *
     * @param url String with URL of the Snowflake server.
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
     * @param database String with database name.
     */
    public DataSourceConfiguration withDatabase(String database) {
      return builder().setDatabase(ValueProvider.StaticValueProvider.of(database)).build();
    }

    public DataSourceConfiguration withDatabase(ValueProvider<String> database) {
      return builder().setDatabase(database).build();
    }

    /**
     * Sets Snowflake Warehouse to use.
     *
     * @param warehouse ValueProvider with warehouse name.
     */
    public DataSourceConfiguration withWarehouse(ValueProvider<String> warehouse) {
      return builder().setWarehouse(warehouse).build();
    }

    /**
     * Sets Snowflake Warehouse to use.
     *
     * @param warehouse String with warehouse name.
     */
    public DataSourceConfiguration withWarehouse(String warehouse) {
      return withWarehouse(ValueProvider.StaticValueProvider.of(warehouse));
    }

    /**
     * Sets schema to use when connecting to Snowflake.
     *
     * @param schema String with schema name.
     */
    public DataSourceConfiguration withSchema(String schema) {
      return builder().setSchema(ValueProvider.StaticValueProvider.of(schema)).build();
    }

    public DataSourceConfiguration withSchema(ValueProvider<String> schema) {
      return builder().setSchema(schema).build();
    }

    /**
     * Sets the name of the Snowflake server. Following format is required:
     * <account_name>.snowflakecomputing.com
     *
     * <p>Either withServerName or withUrl is required.
     *
     * @param serverName String with server name.
     */
    public DataSourceConfiguration withServerName(String serverName) {
      checkArgument(
          serverName.endsWith("snowflakecomputing.com"),
          "serverName must be in format <account_name>.snowflakecomputing.com");
      return withServerName(ValueProvider.StaticValueProvider.of(serverName));
    }

    public DataSourceConfiguration withServerName(ValueProvider<String> serverName) {
      return builder().setServerName(serverName).build();
    }

    /**
     * Sets port number to use to connect to Snowflake.
     *
     * @param portNumber Integer with port number.
     */
    public DataSourceConfiguration withPortNumber(Integer portNumber) {
      return builder().setPortNumber(portNumber).build();
    }

    /**
     * Sets user's role to be used when running queries on Snowflake.
     *
     * @param role ValueProvider with role name.
     */
    public DataSourceConfiguration withRole(ValueProvider<String> role) {
      return builder().setRole(role).build();
    }

    /**
     * Sets user's role to be used when running queries on Snowflake.
     *
     * @param role String with role name.
     */
    public DataSourceConfiguration withRole(String role) {
      return withRole(ValueProvider.StaticValueProvider.of(role));
    }

    /**
     * Sets authenticator for Snowflake.
     *
     * @param authenticator String with authenticator name.
     */
    public DataSourceConfiguration withAuthenticator(String authenticator) {
      return builder().setAuthenticator(authenticator).build();
    }

    /**
     * Sets loginTimeout that will be used in {@link SnowflakeBasicDataSource#setLoginTimeout}.
     *
     * @param loginTimeout Integer with timeout value.
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
        basicDataSource.setUrl(buildUrl());

        if (isNotEmpty(getOauthToken())) {
          basicDataSource.setOauthToken(getOauthToken().get());
        } else if (isNotEmpty(getUsername()) && getPrivateKey() != null) {
          basicDataSource.setUser(getUsername().get());
          basicDataSource.setPrivateKey(getPrivateKey());
        } else if (isNotEmpty(getUsername())
            && isNotEmpty(getPrivateKeyPassphrase())
            && isNotEmpty(getRawPrivateKey())) {
          PrivateKey privateKey =
              KeyPairUtils.preparePrivateKey(
                  getRawPrivateKey().get(), getPrivateKeyPassphrase().get());
          basicDataSource.setPrivateKey(privateKey);
          basicDataSource.setUser(getUsername().get());

        } else if (isNotEmpty(getUsername()) && isNotEmpty(getPassword())) {
          basicDataSource.setUser(getUsername().get());
          basicDataSource.setPassword(getPassword().get());
        } else {
          throw new RuntimeException("Missing credentials values. Please check your credentials");
        }

        if (isNotEmpty(getDatabase())) {
          basicDataSource.setDatabaseName(getDatabase().get());
        }
        if (isNotEmpty(getWarehouse())) {
          basicDataSource.setWarehouse(getWarehouse().get());
        }
        if (isNotEmpty(getSchema())) {
          basicDataSource.setSchema(getSchema().get());
        }
        if (isNotEmpty(getServerName())) {
          basicDataSource.setServerName(getServerName().get());
        }
        if (getPortNumber() != null) {
          basicDataSource.setPortNumber(getPortNumber());
        }
        if (isNotEmpty(getRole())) {
          basicDataSource.setRole(getRole().get());
        }
        if (getAuthenticator() != null) {
          basicDataSource.setAuthenticator(getAuthenticator());
        }
        if (getLoginTimeout() != null) {
          try {
            basicDataSource.setLoginTimeout(getLoginTimeout());
          } catch (SQLException e) {
            throw new RuntimeException("Failed to setLoginTimeout");
          }
        }
        return basicDataSource;
      }
      return getDataSource();
    }

    private String buildUrl() {
      StringBuilder url = new StringBuilder();

      if (getUrl() != null) {
        url.append(getUrl());
      } else {
        url.append("jdbc:snowflake://");
        url.append(getServerName().get());
      }
      if (getPortNumber() != null) {
        url.append(":").append(getPortNumber());
      }
      url.append("?application=beam");
      return url.toString();
    }
  }

  /** Wraps {@link DataSourceConfiguration} to provide DataSource. */
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

    public DataSourceConfiguration getConfig() {
      return this.config;
    }
  }

  private static String getValueOrNull(ValueProvider<String> valueProvider) {
    return valueProvider != null ? valueProvider.get() : null;
  }

  private static boolean isNotEmpty(ValueProvider<String> valueProvider) {
    return valueProvider != null && valueProvider.get() != null && !valueProvider.get().isEmpty();
  }
}
