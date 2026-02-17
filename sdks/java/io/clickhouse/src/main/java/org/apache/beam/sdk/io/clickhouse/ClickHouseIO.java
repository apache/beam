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
package org.apache.beam.sdk.io.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import com.clickhouse.data.ClickHouseFormat;
import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.io.clickhouse.TableSchema.DefaultType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to write to ClickHouse.
 *
 * <h3>Writing to ClickHouse</h3>
 *
 * <p>To write to ClickHouse, use {@link ClickHouseIO#write(String, String, String)}, which writes
 * elements from input {@link PCollection}. It's required that your ClickHouse cluster already has
 * table you are going to insert into.
 *
 * <pre>{@code
 * // New way (recommended):
 * Properties props = new Properties();
 * props.setProperty("user", "admin");
 * props.setProperty("password", "secret");
 *
 * pipeline
 *   .apply(...)
 *   .apply(
 *     ClickHouseIO.<POJO>write("http://localhost:8123", "default", "my_table")
 *       .withProperties(props));
 *
 * // Old way (deprecated):
 * pipeline
 *   .apply(...)
 *   .apply(
 *     ClickHouseIO.<POJO>write("jdbc:clickhouse://localhost:8123/default", "my_table"));
 * }</pre>
 *
 * <p>Optionally, you can provide connection settings, for instance, specify insert block size with
 * {@link Write#withMaxInsertBlockSize(long)}, or configure number of retries with {@link
 * Write#withMaxRetries(int)}.
 *
 * <h4>Deduplication</h4>
 *
 * <p>Deduplication is performed by ClickHouse if inserting to <a
 * href="https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication">ReplicatedMergeTree</a>
 * or <a
 * href="https://clickhouse.com/docs/engines/table-engines/special/distributed">Distributed</a>
 * table on top of ReplicatedMergeTree. Without replication, inserting into regular MergeTree can
 * produce duplicates, if insert fails, and then successfully retries. However, each block is
 * inserted atomically, and you can configure block size with {@link
 * Write#withMaxInsertBlockSize(long)}.
 *
 * <p>Deduplication is performed using checksums of inserted blocks. For <a
 * href="https://clickhouse.com/docs/engines/table-engines/mergetree-family/shared-merge-tree">SharedMergeTree</a>
 * tables in ClickHouse Cloud, deduplication behavior is similar to ReplicatedMergeTree.
 *
 * <h4>Mapping between Beam and ClickHouse types</h4>
 *
 * <table summary="Type mapping">
 * <tr><th>ClickHouse</th> <th>Beam</th></tr>
 * <tr><td>{@link TableSchema.TypeName#FLOAT32}</td> <td>{@link Schema.TypeName#FLOAT}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#FLOAT64}</td> <td>{@link Schema.TypeName#DOUBLE}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#FIXEDSTRING}</td> <td>{@link FixedBytes}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#INT8}</td> <td>{@link Schema.TypeName#BYTE}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#INT16}</td> <td>{@link Schema.TypeName#INT16}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#INT32}</td> <td>{@link Schema.TypeName#INT32}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#INT64}</td> <td>{@link Schema.TypeName#INT64}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#STRING}</td> <td>{@link Schema.TypeName#STRING}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#UINT8}</td> <td>{@link Schema.TypeName#INT16}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#UINT16}</td> <td>{@link Schema.TypeName#INT32}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#UINT32}</td> <td>{@link Schema.TypeName#INT64}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#UINT64}</td> <td>{@link Schema.TypeName#INT64}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#DATE}</td> <td>{@link Schema.TypeName#DATETIME}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#DATETIME}</td> <td>{@link Schema.TypeName#DATETIME}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#ARRAY}</td> <td>{@link Schema.TypeName#ARRAY}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#ENUM8}</td> <td>{@link Schema.TypeName#STRING}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#ENUM16}</td> <td>{@link Schema.TypeName#STRING}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#BOOL}</td> <td>{@link Schema.TypeName#BOOLEAN}</td></tr>
 * <tr><td>{@link TableSchema.TypeName#TUPLE}</td> <td>{@link Schema.TypeName#ROW}</td></tr>
 * </table>
 *
 * <p>Nullable row columns are supported through <a href="https://clickhouse.com/docs/sql-reference/data-types/nullable">Nullable type</a> in ClickHouse.
 * <a href="https://clickhouse.com/docs/sql-reference/data-types/LowCardinality"> Low cardinality hint </a>
 * is supported through LowCardinality DataType in ClickHouse.
 *
 * <p>Nested rows should be unnested using {@link Select#flattenedSchema()}. Type casting should be
 * done using {@link org.apache.beam.sdk.schemas.transforms.Cast} before {@link ClickHouseIO}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ClickHouseIO {

  public static final long DEFAULT_MAX_INSERT_BLOCK_SIZE = 1000000;
  public static final int DEFAULT_MAX_RETRIES = 5;
  public static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardDays(1000);
  public static final Duration DEFAULT_INITIAL_BACKOFF = Duration.standardSeconds(5);

  /**
   * Creates a write transform using a JDBC URL format.
   *
   * <p><b>Deprecated:</b> Use {@link #write(String, String, String)} instead with separate URL,
   * database, and table parameters.
   *
   * <p>This method is provided for backward compatibility. It parses the JDBC URL to extract the
   * connection URL, database name, and any connection properties specified in the query string.
   * Properties can be overridden later using {@link Write#withProperties(Properties)}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * // Old way (deprecated):
   * ClickHouseIO.write("jdbc:clickhouse://localhost:8123/mydb?user=admin&password=secret", "table")
   *
   * // New way:
   * ClickHouseIO.write("http://localhost:8123", "mydb", "table")
   *   .withProperties(props)
   * }</pre>
   *
   * <p><b>Property Precedence:</b> Properties from the JDBC URL can be overridden by calling {@link
   * Write#withProperties(Properties)}. Later calls to withProperties() override earlier settings.
   *
   * @param jdbcUrl JDBC connection URL (e.g., jdbc:clickhouse://host:port/database?param=value)
   * @param table table name
   * @return a {@link PTransform} writing data to ClickHouse
   * @deprecated Use {@link #write(String, String, String)} with explicit URL, database, and table
   */
  @Deprecated
  public static <T> Write<T> write(String jdbcUrl, String table) {
    ClickHouseJdbcUrlParser.ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);

    return new AutoValue_ClickHouseIO_Write.Builder<T>()
        .clickHouseUrl(parsed.getClickHouseUrl())
        .database(parsed.getDatabase())
        .table(table)
        .properties(parsed.getProperties()) // Start with JDBC URL properties
        .maxInsertBlockSize(DEFAULT_MAX_INSERT_BLOCK_SIZE)
        .initialBackoff(DEFAULT_INITIAL_BACKOFF)
        .maxRetries(DEFAULT_MAX_RETRIES)
        .maxCumulativeBackoff(DEFAULT_MAX_CUMULATIVE_BACKOFF)
        .build()
        .withInsertDeduplicate(true)
        .withInsertDistributedSync(true);
  }

  public static <T> Write<T> write(String clickHouseUrl, String database, String table) {
    return new AutoValue_ClickHouseIO_Write.Builder<T>()
        .clickHouseUrl(clickHouseUrl)
        .database(database)
        .table(table)
        .properties(new Properties())
        .maxInsertBlockSize(DEFAULT_MAX_INSERT_BLOCK_SIZE)
        .initialBackoff(DEFAULT_INITIAL_BACKOFF)
        .maxRetries(DEFAULT_MAX_RETRIES)
        .maxCumulativeBackoff(DEFAULT_MAX_CUMULATIVE_BACKOFF)
        .build()
        .withInsertDeduplicate(true)
        .withInsertDistributedSync(true);
  }

  /** A {@link PTransform} to write to ClickHouse. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    public abstract String clickHouseUrl();

    public abstract String database();

    public abstract String table();

    public abstract Properties properties();

    public abstract long maxInsertBlockSize();

    public abstract int maxRetries();

    public abstract Duration maxCumulativeBackoff();

    public abstract Duration initialBackoff();

    public abstract @Nullable TableSchema tableSchema();

    public abstract @Nullable Boolean insertDistributedSync();

    public abstract @Nullable Long insertQuorum();

    public abstract @Nullable Boolean insertDeduplicate();

    abstract Builder<T> toBuilder();

    @Override
    public PDone expand(PCollection<T> input) {
      TableSchema tableSchema = tableSchema();
      if (tableSchema == null) {
        tableSchema = getTableSchema(clickHouseUrl(), database(), table(), properties());
      }

      String sdkVersion = ReleaseInfo.getReleaseInfo().getSdkVersion();
      String userAgent = String.format("Apache Beam/%s", sdkVersion);

      Properties properties = properties();

      set(properties, "max_insert_block_size", maxInsertBlockSize());
      set(properties, "insert_quorum", insertQuorum());
      set(properties, "insert_distributed_sync", insertDistributedSync());
      set(properties, "insert_deduplication", insertDeduplicate());
      set(properties, "product_name", userAgent);

      WriteFn<T> fn =
          new AutoValue_ClickHouseIO_WriteFn.Builder<T>()
              .clickHouseUrl(clickHouseUrl())
              .database(database())
              .table(table())
              .maxInsertBlockSize(maxInsertBlockSize())
              .schema(tableSchema)
              .properties(properties)
              .initialBackoff(initialBackoff())
              .maxCumulativeBackoff(maxCumulativeBackoff())
              .maxRetries(maxRetries())
              .build();

      input.apply(ParDo.of(fn));

      return PDone.in(input.getPipeline());
    }

    /**
     * The maximum block size for insertion, if we control the creation of blocks for insertion.
     *
     * @param value number of rows
     * @return a {@link PTransform} writing data to ClickHouse
     * @see <a href="https://clickhouse.com/docs/operations/settings/settings#max_insert_block_size">ClickHouse
     *     documentation</a>
     */
    public Write<T> withMaxInsertBlockSize(long value) {
      return toBuilder().maxInsertBlockSize(value).build();
    }

    /**
     * If setting is enabled, insert query into distributed waits until data will be sent to all
     * nodes in cluster.
     *
     * @param value true to enable, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInsertDistributedSync(@Nullable Boolean value) {
      return toBuilder().insertDistributedSync(value).build();
    }

    /**
     * For INSERT queries in the replicated table, wait writing for the specified number of replicas
     * and linearize the addition of the data. 0 - disabled.
     *
     * <p>This setting is disabled in default server settings.
     *
     * @param value number of replicas, 0 for disabling, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     * @see <a href="https://clickhouse.com/docs/operations/settings/settings#insert_quorum">ClickHouse
     *     documentation</a>
     */
    public Write<T> withInsertQuorum(@Nullable Long value) {
      return toBuilder().insertQuorum(value).build();
    }

    /**
     * For INSERT queries in the replicated table, specifies that deduplication of inserting blocks
     * should be performed.
     *
     * <p>Enabled by default. Shouldn't be disabled unless your input has duplicate blocks, and you
     * don't want to deduplicate them.
     *
     * @param value true to enable, null for server default
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInsertDeduplicate(Boolean value) {
      return toBuilder().insertDeduplicate(value).build();
    }

    /**
     * Maximum number of retries per insert.
     *
     * <p>See {@link FluentBackoff#withMaxRetries}.
     *
     * @param value maximum number of retries
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withMaxRetries(int value) {
      return toBuilder().maxRetries(value).build();
    }

    /**
     * Limits total time spent in backoff.
     *
     * <p>See {@link FluentBackoff#withMaxCumulativeBackoff}.
     *
     * @param value maximum duration
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withMaxCumulativeBackoff(Duration value) {
      return toBuilder().maxCumulativeBackoff(value).build();
    }

    /**
     * Set initial backoff duration.
     *
     * <p>See {@link FluentBackoff#withInitialBackoff}.
     *
     * @param value initial duration
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withInitialBackoff(Duration value) {
      return toBuilder().initialBackoff(value).build();
    }

    /**
     * Set TableSchema. If not set, then TableSchema will be fetched from clickhouse server itself
     *
     * @param tableSchema schema of Table in which rows are going to be inserted
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withTableSchema(@Nullable TableSchema tableSchema) {
      return toBuilder().tableSchema(tableSchema).build();
    }

    /**
     * Set connection properties (user, password, etc.).
     *
     * <p>Properties set via this method will override any properties previously set, including
     * those extracted from JDBC URLs in the deprecated write method.
     *
     * @param properties connection properties
     * @return a {@link PTransform} writing data to ClickHouse
     */
    public Write<T> withProperties(Properties properties) {

      if (properties == null) {
        throw new IllegalArgumentException("Properties cannot be null");
      }

      // Merge properties: new properties override existing ones
      Properties merged = new Properties();
      merged.putAll(properties()); // Start with existing properties
      merged.putAll(properties); // Override with new properties
      return toBuilder().properties(merged).build();
    }

    /** Builder for {@link Write}. */
    @AutoValue.Builder
    abstract static class Builder<T> {

      public abstract Builder<T> clickHouseUrl(String clickHouseUrl);

      public abstract Builder<T> database(String database);

      public abstract Builder<T> table(String table);

      public abstract Builder<T> maxInsertBlockSize(long maxInsertBlockSize);

      public abstract Builder<T> tableSchema(TableSchema tableSchema);

      public abstract Builder<T> insertDistributedSync(Boolean insertDistributedSync);

      public abstract Builder<T> insertQuorum(Long insertQuorum);

      public abstract Builder<T> insertDeduplicate(Boolean insertDeduplicate);

      public abstract Builder<T> properties(Properties properties);

      public abstract Builder<T> maxRetries(int maxRetries);

      public abstract Builder<T> maxCumulativeBackoff(Duration maxCumulativeBackoff);

      public abstract Builder<T> initialBackoff(Duration initialBackoff);

      public abstract Write<T> build();
    }

    private static void set(Properties properties, String param, Object value) {
      if (value != null) {
        properties.put(param, value);
      }
    }
  }

  @AutoValue
  abstract static class WriteFn<T> extends DoFn<T, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteFn.class);
    private static final String RETRY_ATTEMPT_LOG =
        "Error writing to ClickHouse. Retry attempt[{}]";

    private Client client;
    private FluentBackoff retryBackoff;
    private final List<Row> buffer = new ArrayList<>();
    private final Distribution batchSize = Metrics.distribution(Write.class, "batch_size");
    private final Counter retries = Metrics.counter(Write.class, "retries");

    // TODO: This should be the same as resolved so that Beam knows which fields
    // are being accessed. Currently Beam only supports wildcard descriptors.
    // Once https://github.com/apache/beam/issues/18903 is fixed, fix this.
    @FieldAccess("filterFields")
    final FieldAccessDescriptor fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();

    public abstract String clickHouseUrl();

    public abstract String database();

    public abstract String table();

    public abstract long maxInsertBlockSize();

    public abstract int maxRetries();

    public abstract Duration maxCumulativeBackoff();

    public abstract Duration initialBackoff();

    public abstract TableSchema schema();

    public abstract Properties properties();

    @VisibleForTesting
    static String insertSql(TableSchema schema, String table) {
      String columnsStr =
          schema.columns().stream()
              .filter(x -> !x.materializedOrAlias())
              .map(x -> quoteIdentifier(x.name()))
              .collect(Collectors.joining(", "));
      return "INSERT INTO " + quoteIdentifier(table) + " (" + columnsStr + ")";
    }

    @Setup
    public void setup() throws Exception {

      String user = properties().getProperty("user", "default");
      String password = properties().getProperty("password", "");

      // add the options to the client builder
      Map<String, String> options =
          properties().stringPropertyNames().stream()
              .filter(key -> !key.equals("user") && !key.equals("password"))
              .collect(Collectors.toMap(key -> key, properties()::getProperty));

      // Create ClickHouse Java Client
      Client.Builder clientBuilder =
          new Client.Builder()
              .addEndpoint(clickHouseUrl())
              .setUsername(user)
              .setPassword(password)
              .setDefaultDatabase(database())
              .setOptions(options)
              .setClientName(
                  String.format("Apache Beam/%s", ReleaseInfo.getReleaseInfo().getSdkVersion()));

      // Add optional compression if specified in properties
      String compress = properties().getProperty("compress", "false");
      if (Boolean.parseBoolean(compress)) {
        clientBuilder.compressServerResponse(true);
        clientBuilder.compressClientRequest(true);
      }

      client = clientBuilder.build();

      retryBackoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxRetries())
              .withMaxCumulativeBackoff(maxCumulativeBackoff())
              .withInitialBackoff(initialBackoff());
    }

    @Teardown
    public void tearDown() throws Exception {
      if (client != null) {
        client.close();
      }
    }

    @StartBundle
    public void startBundle() {
      buffer.clear();
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      flush();
    }

    @ProcessElement
    public void processElement(@FieldAccess("filterFields") @Element Row input) throws Exception {
      buffer.add(input);

      if (buffer.size() >= maxInsertBlockSize()) {
        flush();
      }
    }

    private void flush() throws Exception {
      BackOff backOff = retryBackoff.backoff();
      int attempt = 0;

      if (buffer.isEmpty()) {
        return;
      }

      batchSize.update(buffer.size());

      // Serialize rows to RowBinary format
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

      // Wrap ByteArrayOutputStream with ClickHouseOutputStream
      try (com.clickhouse.data.ClickHouseOutputStream outputStream =
          com.clickhouse.data.ClickHouseOutputStream.of(byteStream)) {
        for (Row row : buffer) {
          ClickHouseWriter.writeRow(outputStream, schema(), row);
        }
        outputStream.flush();
      }
      byte[] data = byteStream.toByteArray();

      while (true) {
        try {

          // Perform the insert using ClickHouse Java Client
          InsertResponse response =
              client
                  .insert(
                      table(), new java.io.ByteArrayInputStream(data), ClickHouseFormat.RowBinary)
                  .get();

          if (response != null) {
            LOG.debug(
                "Successfully inserted {} rows out of {} into table {}. total size written {} bytes",
                response.getWrittenRows(),
                buffer.size(),
                table(),
                response.getWrittenBytes());
          } else {
            LOG.debug("Successfully inserted {} rows into table {}", buffer.size(), table());
          }

          buffer.clear();
          break;
        } catch (Exception e) {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
            throw new RuntimeException("Failed to write to ClickHouse after retries", e);
          } else {
            retries.inc();
            LOG.warn(RETRY_ATTEMPT_LOG, attempt, e);
            attempt++;
          }
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      public abstract Builder<T> clickHouseUrl(String clickHouseUrl);

      public abstract Builder<T> database(String database);

      public abstract Builder<T> table(String table);

      public abstract Builder<T> maxInsertBlockSize(long maxInsertBlockSize);

      public abstract Builder<T> schema(TableSchema schema);

      public abstract Builder<T> properties(Properties properties);

      public abstract Builder<T> maxRetries(int maxRetries);

      public abstract Builder<T> maxCumulativeBackoff(Duration maxCumulativeBackoff);

      public abstract Builder<T> initialBackoff(Duration initialBackoff);

      public abstract WriteFn<T> build();
    }
  }

  private static String tuplePreprocessing(String payload) {
    List<String> l =
        Arrays.stream(payload.trim().split(","))
            .map(s -> s.trim().replaceAll(" +", "' "))
            .collect(Collectors.toList());
    String content =
        String.join(",", l).trim().replaceAll("Tuple\\(", "Tuple('").replaceAll(",", ",'");
    return content;
  }

  /**
   * Returns {@link TableSchema} for a given table using JDBC URL format.
   *
   * <p><b>Deprecated:</b> Use {@link #getTableSchema(String, String, String, Properties)} instead
   * with separate URL, database, table, and properties parameters.
   *
   * <p>This method parses the JDBC URL to extract connection details and properties. For new code,
   * use the explicit parameter version for better clarity and control.
   *
   * <p>Example migration:
   *
   * <pre>{@code
   * // Old way (deprecated):
   * TableSchema schema = ClickHouseIO.getTableSchema(
   *     "jdbc:clickhouse://localhost:8123/mydb?user=admin", "my_table");
   *
   * // New way:
   * Properties props = new Properties();
   * props.setProperty("user", "admin");
   * TableSchema schema = ClickHouseIO.getTableSchema(
   *     "http://localhost:8123", "mydb", "my_table", props);
   * }</pre>
   *
   * @param jdbcUrl JDBC connection URL (e.g., jdbc:clickhouse://host:port/database?param=value)
   * @param table table name
   * @return table schema
   * @deprecated Use {@link #getTableSchema(String, String, String, Properties)} with explicit
   *     parameters
   */
  @Deprecated
  public static TableSchema getTableSchema(String jdbcUrl, String table) {
    ClickHouseJdbcUrlParser.ParsedJdbcUrl parsed = ClickHouseJdbcUrlParser.parse(jdbcUrl);
    return getTableSchema(
        parsed.getClickHouseUrl(), parsed.getDatabase(), table, parsed.getProperties());
  }

  /**
   * Returns {@link TableSchema} for a given table using ClickHouse Java Client.
   *
   * @param clickHouseUrl ClickHouse connection url
   * @param database ClickHouse database
   * @param table table name
   * @param properties connection properties
   * @return table schema
   * @since 2.72.0
   */
  public static TableSchema getTableSchema(
      String clickHouseUrl, String database, String table, Properties properties) {
    List<TableSchema.Column> columns = new ArrayList<>();

    try {
      String user = properties.getProperty("user", "default");
      String password = properties.getProperty("password", "");

      // Create ClickHouse Java Client
      Client.Builder clientBuilder =
          new Client.Builder()
              .addEndpoint(clickHouseUrl)
              .setUsername(user)
              .setPassword(password)
              .setDefaultDatabase(database)
              .setClientName(
                  String.format("Apache Beam/%s", ReleaseInfo.getReleaseInfo().getSdkVersion()));

      try (Client client = clientBuilder.build()) {
        String query = "DESCRIBE TABLE " + quoteIdentifier(table);

        try (Records records = client.queryRecords(query).get()) {
          for (GenericRecord record : records) {
            String name = record.getString("name");
            String type = record.getString("type");
            String defaultTypeStr = record.getString("default_type");
            String defaultExpression = record.getString("default_expression");

            ColumnType columnType;
            if (type.toLowerCase().trim().startsWith("tuple(")) {
              String content = tuplePreprocessing(type);
              columnType = ColumnType.parse(content);
            } else {
              columnType = ColumnType.parse(type);
            }
            DefaultType defaultType = DefaultType.parse(defaultTypeStr).orElse(null);

            Object defaultValue;
            if (DefaultType.DEFAULT.equals(defaultType)
                && !Strings.isNullOrEmpty(defaultExpression)) {
              defaultValue = ColumnType.parseDefaultExpression(columnType, defaultExpression);
            } else {
              defaultValue = null;
            }

            columns.add(TableSchema.Column.of(name, columnType, defaultType, defaultValue));
          }
        }
      }

      return TableSchema.of(columns.toArray(new TableSchema.Column[0]));
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table schema for table: " + table, e);
    }
  }

  static String quoteIdentifier(String identifier) {
    String backslash = "\\\\";
    String quote = "\"";

    return quote + identifier.replaceAll(quote, backslash + quote) + quote;
  }
}
