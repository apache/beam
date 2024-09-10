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

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.google.auto.value.AutoValue;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * <p>To write to ClickHouse, use {@link ClickHouseIO#write(String, String)}, which writes elements
 * from input {@link PCollection}. It's required that your ClickHouse cluster already has table you
 * are going to insert into.
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(
 *     ClickHouseIO.<POJO>write("jdbc:clickhouse:localhost:8123/default", "my_table"));
 * }</pre>
 *
 * <p>Optionally, you can provide connection settings, for instance, specify insert block size with
 * {@link Write#withMaxInsertBlockSize(long)}, or configure number of retries with {@link
 * Write#withMaxRetries(int)}.
 *
 * <h4>Deduplication</h4>
 *
 * Deduplication is performed by ClickHouse if inserting to <a
 * href="https://clickhouse.yandex/docs/en/single/#data-replication">ReplicatedMergeTree</a> or <a
 * href="https://clickhouse.yandex/docs/en/single/#distributed">Distributed</a> table on top of
 * ReplicatedMergeTree. Without replication, inserting into regular MergeTree can produce
 * duplicates, if insert fails, and then successfully retries. However, each block is inserted
 * atomically, and you can configure block size with {@link Write#withMaxInsertBlockSize(long)}.
 *
 * <p>Deduplication is performed using checksums of inserted blocks.
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
 * Nullable row columns are supported through Nullable type in ClickHouse. Low cardinality hint is
 * supported through LowCardinality DataType in ClickHouse.
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

  public static <T> Write<T> write(String jdbcUrl, String table) {
    return new AutoValue_ClickHouseIO_Write.Builder<T>()
        .jdbcUrl(jdbcUrl)
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

    public abstract String jdbcUrl();

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
        tableSchema = getTableSchema(jdbcUrl(), table());
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
              .jdbcUrl(jdbcUrl())
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
     * @see <a href="https://clickhouse.yandex/docs/en/single/#max_insert_block_size">ClickHouse
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
     * @see <a href="https://clickhouse.yandex/docs/en/single/#insert_quorum">ClickHouse
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

    /** Builder for {@link Write}. */
    @AutoValue.Builder
    abstract static class Builder<T> {

      public abstract Builder<T> jdbcUrl(String jdbcUrl);

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

    private ClickHouseConnection connection;
    private FluentBackoff retryBackoff;
    private final List<Row> buffer = new ArrayList<>();
    private final Distribution batchSize = Metrics.distribution(Write.class, "batch_size");
    private final Counter retries = Metrics.counter(Write.class, "retries");

    // TODO: This should be the same as resolved so that Beam knows which fields
    // are being accessed. Currently Beam only supports wildcard descriptors.
    // Once https://github.com/apache/beam/issues/18903 is fixed, fix this.
    @FieldAccess("filterFields")
    final FieldAccessDescriptor fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();

    public abstract String jdbcUrl();

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
    public void setup() throws SQLException {

      connection = new ClickHouseDataSource(jdbcUrl(), properties()).getConnection();

      retryBackoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxRetries())
              .withMaxCumulativeBackoff(maxCumulativeBackoff())
              .withInitialBackoff(initialBackoff());
    }

    @Teardown
    public void tearDown() throws Exception {
      connection.close();
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
      while (true) {
        try (ClickHouseStatement statement = connection.createStatement()) {
          statement
              .unwrap(ClickHouseRequest.class)
              .write()
              .table(table())
              .format(ClickHouseFormat.RowBinary)
              .data(
                  out -> {
                    for (Row row : buffer) {
                      ClickHouseWriter.writeRow(out, schema(), row);
                    }
                  })
              .executeAndWait(); // query happens in a separate thread
          buffer.clear();
          break;
        } catch (SQLException e) {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
            throw e;
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

      public abstract Builder<T> jdbcUrl(String jdbcUrl);

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
   * Returns {@link TableSchema} for a given table.
   *
   * @param jdbcUrl jdbc connection url
   * @param table table name
   * @return table schema
   */
  public static TableSchema getTableSchema(String jdbcUrl, String table) {
    List<TableSchema.Column> columns = new ArrayList<>();

    try (ClickHouseConnection connection = new ClickHouseDataSource(jdbcUrl).getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet rs = null; // try-finally is used because findbugs doesn't like try-with-resource
      try {
        rs = statement.executeQuery("DESCRIBE TABLE " + quoteIdentifier(table));

        while (rs.next()) {
          String name = rs.getString("name");
          String type = rs.getString("type");
          String defaultTypeStr = rs.getString("default_type");
          String defaultExpression = rs.getString("default_expression");

          ColumnType columnType = null;
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
      } finally {
        if (rs != null) {
          rs.close();
        }
      }

      return TableSchema.of(columns.toArray(new TableSchema.Column[0]));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static String quoteIdentifier(String identifier) {
    String backslash = "\\\\";
    String quote = "\"";

    return quote + identifier.replaceAll(quote, backslash + quote) + quote;
  }
}
