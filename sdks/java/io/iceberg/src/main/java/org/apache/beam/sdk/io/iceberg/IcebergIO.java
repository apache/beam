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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.iceberg.cdc.IncrementalChangelogSource;
import org.apache.beam.sdk.io.iceberg.cdc.sink.WriteCdcRows;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A connector that reads and writes to <a href="https://iceberg.apache.org/">Apache Iceberg</a>
 * tables.
 *
 * <p>{@link IcebergIO} is offered as a Managed transform. This class is subject to change and
 * should not be used directly. Instead, use it like so:
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", table,
 *         "catalog_name", name,
 *         "catalog_properties", Map.of(
 *                 "warehouse", warehouse_path,
 *                 "catalog-impl", "org.apache.iceberg.hive.HiveCatalog"),
 *         "config_properties", Map.of(
 *                 "hive.metastore.uris", metastore_uri));
 *
 *
 * ====== WRITE ======
 * pipeline
 *     .apply(Create.of(BEAM_ROWS))
 *     .apply(Managed.write(ICEBERG).withConfig(config));
 *
 *
 * ====== READ ======
 * pipeline
 *     .apply(Managed.read(ICEBERG).withConfig(config))
 *     .getSinglePCollection()
 *     .apply(ParDo.of(...));
 *
 *
 * ====== READ CDC ======
 * pipeline
 *     .apply(Managed.read(ICEBERG_CDC).withConfig(config))
 *     .getSinglePCollection()
 *     .apply(ParDo.of(...));
 * }</pre>
 *
 * Look for more detailed examples below.
 *
 * <h2>Configuration Options</h2>
 *
 * Please check the <a href="https://beam.apache.org/documentation/io/managed-io/">Managed IO
 * configuration page</a>
 *
 * <h3>Beam Rows</h3>
 *
 * <p>Being a Managed transform, this IO exclusively writes and reads using Beam {@link Row}s.
 * Conversion takes place between Beam {@link Row}s and Iceberg {@link Record}s using helper methods
 * in {@link IcebergUtils}. Below is the mapping between Beam and Iceberg types:
 *
 * <table border="1" cellspacing="1">
 *   <tr>
 *     <td> <b>Beam {@link Schema.FieldType}</b> </td> <td> <b>Iceberg {@link Type}</b>
 *   </tr>
 *   <tr>
 *     <td> BYTES </td> <td> BINARY </td>
 *   </tr>
 *   <tr>
 *     <td> BOOLEAN </td> <td> BOOLEAN </td>
 *   </tr>
 *   <tr>
 *     <td> STRING </td> <td> STRING </td>
 *   </tr>
 *   <tr>
 *     <td> INT32 </td> <td> INTEGER </td>
 *   </tr>
 *   <tr>
 *     <td> INT64 </td> <td> LONG </td>
 *   </tr>
 *   <tr>
 *     <td> DECIMAL </td> <td> STRING </td>
 *   </tr>
 *   <tr>
 *     <td> FLOAT </td> <td> FLOAT </td>
 *   </tr>
 *   <tr>
 *     <td> DOUBLE </td> <td> DOUBLE </td>
 *   </tr>
 *   <tr>
 *     <td> SqlTypes.DATETIME </td> <td> TIMESTAMP </td>
 *   </tr>
 *   <tr>
 *     <td> DATETIME </td> <td> TIMESTAMPTZ </td>
 *   </tr>
 *   <tr>
 *     <td> SqlTypes.DATE </td> <td> DATE </td>
 *   </tr>
 *   <tr>
 *     <td> SqlTypes.TIME </td> <td> TIME </td>
 *   </tr>
 *   <tr>
 *     <td> ITERABLE </td> <td> LIST </td>
 *   </tr>
 *   <tr>
 *     <td> ARRAY </td> <td> LIST </td>
 *   </tr>
 *   <tr>
 *     <td> MAP </td> <td> MAP </td>
 *   </tr>
 *   <tr>
 *     <td> ROW </td> <td> STRUCT </td>
 *   </tr>
 * </table>
 *
 * <p><b>Note:</b> {@code SqlTypes} are Beam logical types.
 *
 * <h3>Note on timestamps</h3>
 *
 * <p>For an existing table, the following Beam types are supported for both {@code timestamp} and
 * {@code timestamptz}:
 *
 * <ul>
 *   <li>{@code SqlTypes.DATETIME} --> Using a {@link java.time.LocalDateTime} object
 *   <li>{@code DATETIME} --> Using a {@link org.joda.time.DateTime} object
 *   <li>{@code INT64} --> Using a {@link Long} representing micros since EPOCH
 *   <li>{@code STRING} --> Using a timestamp {@link String} representation (e.g. {@code
 *       "2024-10-08T13:18:20.053+03:27"})
 * </ul>
 *
 * <p><b>Note</b>: If you expect Beam to create the Iceberg table at runtime, please provide {@code
 * SqlTypes.DATETIME} for a {@code timestamp} column and {@code DATETIME} for a {@code timestamptz}
 * column. If the table does not exist, Beam will treat {@code STRING} and {@code INT64} at
 * face-value and create equivalent column types.
 *
 * <p>For Iceberg reads, the connector will produce Beam {@code SqlTypes.DATETIME} types for
 * Iceberg's {@code timestamp} and {@code DATETIME} types for {@code timestamptz}.
 *
 * <h2>Writing to Tables</h2>
 *
 * <h3>Creating Tables</h3>
 *
 * <p>If an Iceberg table does not exist at the time of writing, this connector will automatically
 * create one with the data's schema.
 *
 * <p>Note that this is a best-effort operation that depends on the {@link Catalog} implementation.
 * Some implementations may not support creating a table using the Iceberg API.
 *
 * <h3>Dynamic Destinations</h3>
 *
 * <p>Managed Iceberg supports writing to dynamic destinations. To do so, please provide an
 * identifier template for the <b>{@code table}</b> parameter. A template should have placeholders
 * represented as curly braces containing a record field name, e.g.: {@code
 * "my_namespace.my_{foo}_table"}.
 *
 * <p>The sink uses simple String interpolation to determine a record's table destination. The
 * placeholder is replaced with the record's field value. Nested fields can be specified using
 * dot-notation (e.g. {@code "{top.middle.nested}"}).
 *
 * <h4>Pre-filtering Options</h4>
 *
 * <p>Some use cases may benefit from filtering record fields right before the write operation. For
 * example, you may want to provide meta-data to guide records to the right destination, but not
 * necessarily write that meta-data to your table. Some light-weight filtering options are provided
 * to accommodate such cases, allowing you to control what actually gets written (see <b>{@code
 * drop}</b>, <b>{@code keep}</b>, <b>{@code only}</b>}).
 *
 * <p>Example write to dynamic destinations (pseudocode):
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", "flights.{country}.{airport}",
 *         "catalog_properties", Map.of(...),
 *         "drop", ["country", "airport"]);
 *
 * JSON_ROWS = [
 *       // first record is written to table "flights.usa.RDU"
 *         "{\"country\": \"usa\"," +
 *          "\"airport\": \"RDU\"," +
 *          "\"flight_id\": \"AA356\"," +
 *          "\"destination\": \"SFO\"," +
 *          "\"meal\": \"chicken alfredo\"}",
 *       // second record is written to table "flights.qatar.HIA"
 *         "{\"country\": \"qatar\"," +
 *          "\"airport\": \"HIA\"," +
 *          "\"flight_id\": \"QR 875\"," +
 *          "\"destination\": \"DEL\"," +
 *          "\"meal\": \"shawarma\"}",
 *          ...
 *          ];
 *
 * // fields "country" and "airport" are dropped before
 * // records are written to tables
 * pipeline
 *     .apply(Create.of(JSON_ROWS))
 *     .apply(JsonToRow.withSchema(...))
 *     .apply(Managed.write(ICEBERG).withConfig(config));
 *
 * }</pre>
 *
 * <h3>Sorting and Distribution Modes</h3>
 *
 * <p>You can control how records are grouped, sharded, and sorted before writing to Iceberg data
 * files using <b>{@code distribution_mode}</b>, <b>{@code sort_fields}</b>, <b>{@code
 * sort_on_write}</b>, and <b>{@code autosharding}</b>:
 *
 * <ul>
 *   <li><b>{@code distribution_mode}</b>: {@code "none"} (default; writes directly within worker
 *       bundles without a shuffle), {@code "hash"} (shuffles by Iceberg partition key), or {@code
 *       "range"} (shuffles by partition key and sort-order key). In batch mode, {@code "range"}
 *       samples sort keys per partition to compute balanced quantile boundaries sized to {@code
 *       write.target-file-size-bytes}.
 *   <li><b>{@code sort_fields}</b>: List of sort expressions used when creating a new table (e.g.,
 *       {@code ["ts DESC NULLS LAST", "id ASC"]}, {@code "bucket(id, 16) ASC"}).
 *   <li><b>{@code sort_on_write}</b>: When {@code true}, sorts records within each output file
 *       using the table's {@link org.apache.iceberg.SortOrder} via external merge sort (spilling to
 *       local disk when buffered records exceed memory limits).
 *   <li><b>{@code autosharding}</b>: Dynamically scales the number of parallel writers per
 *       partition when using {@code "hash"} or {@code "range"}.
 * </ul>
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", "my_db.events",
 *         "catalog_properties", Map.of(...),
 *         "partition_fields", List.of("day(ts)"),
 *         "sort_fields", List.of("ts ASC NULLS LAST", "user_id ASC"),
 *         "sort_on_write", true,
 *         "distribution_mode", "range");
 *
 * pipeline
 *     .apply(Managed.write(ICEBERG).withConfig(config));
 * }</pre>
 *
 * <p>See {@link WriteRows#withDistributionMode(DistributionMode)} for a detailed comparison of
 * distribution modes and sorting trade-offs.
 *
 * <h3>Output Snapshots</h3>
 *
 * <p>When records are written and committed to a table, a snapshot is produced. A batch pipeline
 * will perform a single commit and create a single snapshot per table. A streaming pipeline will
 * produce a snapshot roughly according to the configured <b>{@code
 * triggering_frequency_seconds}</b>.
 *
 * <p>You can access these snapshots and perform downstream processing by fetching the <b>{@code
 * "snapshots"}</b> output PCollection:
 *
 * <pre>{@code
 * pipeline
 *     .apply(Create.of(BEAM_ROWS))
 *     .apply(Managed.write(ICEBERG).withConfig(config))
 *     .get("snapshots")
 *     .apply(ParDo.of(new DoFn<Row, T> {...});
 * }</pre>
 *
 * Each Snapshot is represented as a Beam Row, with the following Schema:
 *
 * <table border="1" cellspacing="1">
 *   <tr>
 *     <td> <b>Field</b> </td> <td> <b>Type</b> </td> <td> <b>Description</b> </td>
 *   </tr>
 *   <tr>
 *     <td> {@code table} </td> <td> {@code str} </td> <td> Table identifier. </td>
 *   </tr>
 *   <tr>
 *     <td> {@code manifest_list_location} </td> <td> {@code str} </td> <td> Location of the snapshot's manifest list. </td>
 *   </tr>
 *   <tr>
 *     <td> {@code operation} </td> <td> {@code str} </td> <td> Name of the operation that produced the snapshot. </td>
 *   </tr>
 * <tr>
 *     <td> {@code parent_id} </td> <td> {@code long} </td> <td> The snapshot's parent ID. </td>
 *   </tr>
 *   <tr>
 *     <td> {@code schema_id} </td> <td> {@code int} </td> <td> The id of the schema used when the snapshot was created. </td>
 *   </tr>
 * <tr>
 *     <td> {@code summary} </td> <td> {@code map<str, str>} </td> <td> A string map of summary data. </td>
 *   </tr>
 *   <tr>
 *     <td> {@code timestamp_millis} </td> <td> {@code long} </td> <td> The snapshot's timestamp in milliseconds. </td>
 *   </tr>
 * </table>
 *
 * <br>
 * <br>
 *
 * <h2>Reading from Tables</h2>
 *
 * With the following configuration,
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", table,
 *         "catalog_name", name,
 *         "catalog_properties", Map.of(...),
 *         "config_properties", Map.of(...));
 * }</pre>
 *
 * Example of a simple batch read:
 *
 * <pre>{@code
 * PCollection<Row> rows = pipeline
 *     .apply(Managed.read(ICEBERG).withConfig(config))
 *     .getSinglePCollection();
 * }</pre>
 *
 * Example of a simple CDC streaming read:
 *
 * <pre>{@code
 * PCollection<Row> rows = pipeline
 *     .apply(Managed.read(ICEBERG_CDC).withConfig(config))
 *     .getSinglePCollection();
 * }</pre>
 *
 * <p><b>Note</b>: This reads <b>append-only</b> snapshots. Full CDC is not supported yet.
 *
 * <p>The CDC <b>streaming</b> source (enabled with {@code streaming=true}) continuously polls the
 * table for new snapshots, with a default interval of 60 seconds. This can be overridden with
 * <b>{@code poll_interval_seconds}</b>:
 *
 * <pre>{@code
 * config.put("streaming", true);
 * config.put("poll_interval_seconds", 10);
 * }</pre>
 *
 * <h3>Choosing a Starting Point (ICEBERG_CDC only)</h3>
 *
 * By default, a batch read will start reading from the earliest (oldest) table snapshot. A
 * streaming read will start reading from the latest (most recent) snapshot. This behavior can be
 * overridden in a few <b>mutually exclusive</b> ways:
 *
 * <ul>
 *   <li>Manually setting a starting strategy with <b>{@code starting_strategy}</b> to be {@code
 *       "earliest"} or {@code "latest"}.
 *   <li>Setting a starting snapshot id with <b>{@code from_snapshot}</b>.
 *   <li>Setting a starting timestamp (milliseconds) with <b>{@code from_timestamp}</b>.
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", table,
 *         "catalog_name", name,
 *         "catalog_properties", Map.of(...),
 *         "config_properties", Map.of(...),
 *         "streaming", true,
 *         "from_snapshot", 123456789L);
 *
 * PCollection<Row> = pipeline
 *     .apply(Managed.read(ICEBERG_CDC).withConfig(config))
 *     .getSinglePCollection();
 * }</pre>
 *
 * <h3>Choosing an End Point (ICEBERG_CDC only)</h3>
 *
 * By default, a batch read will go up until the most recent table snapshot. A streaming read will
 * continue monitoring the table for new snapshots forever. This can be overridden with one of the
 * following options:
 *
 * <ul>
 *   <li>Setting an ending snapshot id with <b>{@code to_snapshot}</b>.
 *   <li>Setting an ending timestamp (milliseconds) with <b>{@code to_timestamp}</b>.
 * </ul>
 *
 * <p>For example:
 *
 * <pre>{@code
 * Map<String, Object> config = Map.of(
 *         "table", table,
 *         "catalog_name", name,
 *         "catalog_properties", Map.of(...),
 *         "config_properties", Map.of(...),
 *         "from_snapshot", 123456789L,
 *         "to_timestamp", 987654321L);
 *
 * PCollection<Row> = pipeline
 *     .apply(Managed.read(ICEBERG_CDC).withConfig(config))
 *     .getSinglePCollection();
 * }</pre>
 *
 * <b>Note</b>: If <b>{@code streaming=true}</b> and an end point is set, the pipeline will run in
 * streaming mode and shut down automatically after processing the final snapshot.
 */
@Internal
public class IcebergIO {

  public static WriteRows writeRows(IcebergCatalogConfig catalog) {
    return new AutoValue_IcebergIO_WriteRows.Builder()
        .setCatalogConfig(catalog)
        .setDistributionMode(DistributionMode.NONE)
        .setAutoSharding(false)
        .setUseSideInputTableCache(false)
        .setSortedWrites(false)
        .build();
  }

  /**
   * Returns a {@link WriteCdcRows} transform: the CDC sink, applying inserts/updates/deletes from a
   * {@code PCollection<Row>} of change records (each carrying a {@link
   * org.apache.beam.sdk.values.ValueKind}) to one or more Iceberg V2+ tables via equality deletes;
   * superseded rows are never written.
   *
   * <pre>{@code
   * input.apply(IcebergIO.writeCdcRows(catalogConfig)
   *     .to(tableId)
   *     .withSequenceNumberColumn("seq")
   *     .withTriggeringFrequency(Duration.standardMinutes(1)));
   * }</pre>
   */
  public static WriteCdcRows writeCdcRows(IcebergCatalogConfig catalog) {
    return WriteCdcRows.of(catalog);
  }

  @AutoValue
  public abstract static class WriteRows extends PTransform<PCollection<Row>, IcebergWriteResult> {

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract @Nullable DynamicDestinations getDynamicDestinations();

    abstract @Nullable Duration getTriggeringFrequency();

    abstract @Nullable Integer getDirectWriteByteLimit();

    abstract DistributionMode getDistributionMode();

    abstract @Nullable SerializableFunction<Row, Integer> getDistributionFunction();

    abstract boolean getAutoSharding();

    abstract boolean getSortedWrites();

    abstract @Nullable Map<String, String> getWriteProperties();

    abstract @Nullable List<String> getPartitionFields();

    abstract @Nullable List<String> getSortFields();

    abstract boolean getUseSideInputTableCache();

    abstract @Nullable Integer getMaximumTableCacheSize();

    abstract @Nullable Duration getTableCacheRefreshInterval();

    abstract @Nullable Integer getTableCachePollingBuckets();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract Builder setDynamicDestinations(DynamicDestinations destinations);

      abstract Builder setTriggeringFrequency(Duration triggeringFrequency);

      abstract Builder setDirectWriteByteLimit(Integer directWriteByteLimit);

      abstract Builder setDistributionMode(DistributionMode mode);

      abstract Builder setDistributionFunction(SerializableFunction<Row, Integer> shardFn);

      abstract Builder setAutoSharding(boolean autoSharding);

      abstract Builder setSortedWrites(boolean sortedWrites);

      abstract Builder setWriteProperties(Map<String, String> writeProperties);

      abstract Builder setPartitionFields(List<String> partitionFields);

      abstract Builder setSortFields(List<String> sortFields);

      abstract Builder setUseSideInputTableCache(boolean useSideInputTableCache);

      abstract Builder setMaximumTableCacheSize(@Nullable Integer maximumTableCacheSize);

      abstract Builder setTableCacheRefreshInterval(@Nullable Duration refreshInterval);

      abstract Builder setTableCachePollingBuckets(@Nullable Integer pollingBuckets);

      abstract WriteRows build();
    }

    public WriteRows to(TableIdentifier identifier) {
      return toBuilder().setTableIdentifier(identifier).build();
    }

    public WriteRows to(DynamicDestinations destinations) {
      return toBuilder().setDynamicDestinations(destinations).build();
    }

    /**
     * Sets the frequency at which data is written to files and a new {@link
     * org.apache.iceberg.Snapshot} is produced.
     *
     * <p>Roughly every triggeringFrequency duration, records are written to data files and appended
     * to the respective table. Each append operation creates a new table snapshot.
     *
     * <p>Generally speaking, increasing this duration will result in fewer, larger data files and
     * fewer snapshots.
     *
     * <p>This is only applicable when writing an unbounded {@link PCollection} (i.e. a streaming
     * pipeline).
     */
    public WriteRows withTriggeringFrequency(Duration triggeringFrequency) {
      return toBuilder().setTriggeringFrequency(triggeringFrequency).build();
    }

    public WriteRows withDirectWriteByteLimit(Integer directWriteByteLimit) {
      return toBuilder().setDirectWriteByteLimit(directWriteByteLimit).build();
    }

    /**
     * The default distribution mode is {@link DistributionMode#NONE}, which writes directly within
     * each worker bundle without a partition shuffle. When {@link #withSortedWrites()} is enabled
     * on a table with a {@link org.apache.iceberg.SortOrder}, records are buffered and sorted
     * locally per bundle by {@code (partition, sortKey)} before writing (or per triggered batch in
     * streaming).
     *
     * <p><b>Warning on HASH mode:</b> Utilizing {@code HASH} distribution mode (with or without
     * auto-sharding) can suffer from large unpartitioned or skewed writes if key spaces are not
     * uniformly distributed. This can bottleneck workers and produce fragmented layout files.
     *
     * <p><b>Note on RANGE mode:</b> {@code RANGE} mode automatically enables sorted writes when the
     * target table has a {@link org.apache.iceberg.SortOrder}. In bounded (batch) pipelines, if no
     * {@link #withDistributionFunction} is supplied, {@code RANGE} mode automatically samples sort
     * keys and partition byte volume to compute non-overlapping quantile range shards sized to the
     * target file size. Users may also supply a custom {@link #withDistributionFunction} to control
     * range shard assignment manually (or for streaming pipelines).
     *
     * <h3>Comparison of Distribution Modes:</h3>
     *
     * <table border="1">
     *   <caption>Comparison of Distribution Modes</caption>
     *   <tr>
     *     <td><b>Mode</b></td>
     *     <td><b>Description</b></td>
     *     <td><b>Pros</b></td>
     *     <td><b>Cons</b></td>
     *   </tr>
     *   <tr>
     *     <td>{@link DistributionMode#NONE}</td>
     *     <td>No partition shuffle is performed. When {@link #withSortedWrites()} is enabled on a sorted table, records are sorted locally per bundle by partition and sort key prior to writing.</td>
     *     <td>Highly lightweight with zero network shuffle overhead.</td>
     *     <td>Writers on different workers can write to the same partition or overlapping min/max key ranges across multiple files. Relies on pre-aligned input bundles or downstream compaction.</td>
     *   </tr>
     *   <tr>
     *     <td>{@link DistributionMode#HASH}</td>
     *     <td>Data is shuffled and consolidated by partition key. All records for a partition are routed to a single worker (or dynamically sub-sharded when {@link #withAutosharding()} is enabled).</td>
     *     <td>Consolidates partition files, eliminating cross-worker file fragmentation for partition keys.</td>
     *     <td>Without {@link #withAutosharding()}, can suffer from data skew if a single partition contains significantly more data than others (hot partitions).</td>
     *   </tr>
     *   <tr>
     *     <td>{@link DistributionMode#RANGE}</td>
     *     <td>Data is range-partitioned along the table's sort order (dynamically via quantile sampling in batch by default, or via a custom {@link #withDistributionFunction}) and sorted within each shard.</td>
     *     <td>Distributes writes for hot partitions across multiple workers while keeping file min/max key ranges tight and non-overlapping.</td>
     *     <td>Incurs a range-sampling pass and shuffle in batch; requires a user-supplied {@link #withDistributionFunction} to fan out hot partitions in streaming.</td>
     *   </tr>
     * </table>
     *
     * <h3>Recommendation Matrix (Sorting &amp; Partitioning vs. Scale):</h3>
     *
     * <table border="1">
     *   <caption>Recommendation Matrix</caption>
     *   <tr>
     *     <td><b>Partitioning</b></td>
     *     <td><b>Sorting</b></td>
     *     <td><b>Scale / Volume</b></td>
     *     <td><b>Latency Priority</b></td>
     *     <td><b>Recommended Mode</b></td>
     *     <td><b>Operational Impact</b></td>
     *   </tr>
     *   <tr>
     *     <td>Partitioned</td>
     *     <td>Sorted</td>
     *     <td>Small</td>
     *     <td>Any</td>
     *     <td>{@link DistributionMode#HASH} + {@link #withSortedWrites()}</td>
     *     <td>Consolidates partition files and sorts them locally. Avoids file overlaps for small volumes.</td>
     *   </tr>
     *   <tr>
     *     <td>Partitioned</td>
     *     <td>Sorted</td>
     *     <td>Medium / Large</td>
     *     <td>Low Write Latency</td>
     *     <td>{@link DistributionMode#NONE} + {@link #withSortedWrites()}</td>
     *     <td>Sorts locally per bundle without a partition shuffle. Results in overlapping key ranges across files unless upstream bundles are pre-partitioned, requiring downstream compaction.</td>
     *   </tr>
     *   <tr>
     *     <td>Partitioned</td>
     *     <td>Sorted</td>
     *     <td>Medium / Large</td>
     *     <td>Low Read Latency</td>
     *     <td>{@link DistributionMode#RANGE} OR {@link DistributionMode#HASH} with auto-sharding + {@link #withSortedWrites()}</td>
     *     <td>RANGE sharding distributes hot partitions into sequential, non-overlapping files to optimize reads. HASH with auto-sharding scales writes for hot partitions in streaming/batch but can result in overlapping file ranges requiring query-time sort merges.</td>
     *   </tr>
     *   <tr>
     *     <td>Partitioned</td>
     *     <td>Unsorted</td>
     *     <td>Small</td>
     *     <td>Any</td>
     *     <td>{@link DistributionMode#HASH}</td>
     *     <td>Consolidates data files into single partition directories to prevent file fragmentation.</td>
     *   </tr>
     *   <tr>
     *     <td>Partitioned</td>
     *     <td>Unsorted</td>
     *     <td>Medium / Large</td>
     *     <td>Any</td>
     *     <td>{@link DistributionMode#HASH} with auto-sharding</td>
     *     <td>Consolidates partition files while dynamically balancing hot partition writes across parallel workers.</td>
     *   </tr>
     *   <tr>
     *     <td>Unpartitioned</td>
     *     <td>Sorted</td>
     *     <td>Small</td>
     *     <td>Any</td>
     *     <td>{@link DistributionMode#NONE} + {@link #withSortedWrites()}</td>
     *     <td>Sorts locally per bundle by sort key with zero shuffle overhead.</td>
     *   </tr>
     *   <tr>
     *     <td>Unpartitioned</td>
     *     <td>Sorted</td>
     *     <td>Medium / Large</td>
     *     <td>Low Write Latency</td>
     *     <td>{@link DistributionMode#NONE} + {@link #withSortedWrites()}</td>
     *     <td>Parallel worker writes sorted locally per bundle. Requires downstream compaction to resolve overlapping file ranges across bundles.</td>
     *   </tr>
     *   <tr>
     *     <td>Unpartitioned</td>
     *     <td>Sorted</td>
     *     <td>Medium / Large</td>
     *     <td>Low Read Latency</td>
     *     <td>{@link DistributionMode#RANGE}</td>
     *     <td>Shards continuous keys into non-overlapping worker ranges (dynamically via quantile sampling in batch, or via a custom sharding function). Eliminates single-worker bottlenecks and guarantees zero file overlap for fast queries.</td>
     *   </tr>
     *   <tr>
     *     <td>Unpartitioned</td>
     *     <td>Unsorted</td>
     *     <td>Any</td>
     *     <td>Any</td>
     *     <td>{@link DistributionMode#NONE}</td>
     *     <td>Direct, parallel worker writes with maximum throughput and zero network shuffle overhead.</td>
     *   </tr>
     * </table>
     *
     * <h3>Code Samples:</h3>
     *
     * <pre>{@code
     * // 1. Using the default NONE distribution mode
     * pipeline
     *     .apply(Create.of(BEAM_ROWS))
     *     .apply(IcebergIO.writeRows(catalogConfig)
     *         .to(tableId));
     *
     * // 2. Using HASH distribution mode with sorted writes
     * pipeline
     *     .apply(Create.of(BEAM_ROWS))
     *     .apply(IcebergIO.writeRows(catalogConfig)
     *         .to(tableId)
     *         .withDistributionMode(DistributionMode.HASH)
     *         .withSortedWrites());
     *
     * // 3. Using RANGE distribution mode (automatically computes quantile shards in batch,
     * //    or optionally accepts a custom shard function)
     * pipeline
     *     .apply(Create.of(BEAM_ROWS))
     *     .apply(IcebergIO.writeRows(catalogConfig)
     *         .to(tableId)
     *         .withDistributionMode(DistributionMode.RANGE));
     * }</pre>
     */
    public WriteRows withDistributionMode(DistributionMode mode) {
      return toBuilder().setDistributionMode(mode).build();
    }

    /**
     * Sets an optional custom range-distribution function.
     *
     * <p>Only applicable when the distribution mode is set to {@link DistributionMode#RANGE}. The
     * function maps a Beam {@link Row} to an Integer representing a shard/bucket ID. If omitted in
     * {@link DistributionMode#RANGE}, Beam dynamically computes range shards from the table's sort
     * order and partition byte volume.
     */
    public WriteRows withDistributionFunction(SerializableFunction<Row, Integer> shardFn) {
      return toBuilder().setDistributionFunction(shardFn).build();
    }

    /**
     * Enables Beam's dynamic auto-sharding when using {@link DistributionMode#HASH}.
     *
     * <p>When enabled, the pipeline uses {@link
     * org.apache.beam.sdk.transforms.GroupIntoBatches#withShardedKey()} under the hood. The runner
     * (such as Dataflow) dynamically monitors throughput per partition key. If a partition is
     * extremely hot, the runner automatically splits it into parallel sub-shards distributed across
     * multiple workers to prevent single-worker bottlenecks and out-of-memory (OOM) errors, while
     * keeping the number of data files for cold partitions minimal.
     *
     * <p>Note that because auto-sharding distributes hot-partition data randomly across worker
     * shards, the written data files cannot guarantee non-overlapping key ranges. Downstream
     * queries may require read-time sort merges for overlapping file segments until an Iceberg
     * compaction job (e.g., `rewriteDataFiles`) is executed.
     *
     * <p>Only applicable when using {@link DistributionMode#HASH}.
     */
    public WriteRows withAutosharding() {
      return toBuilder().setAutoSharding(true).build();
    }

    /**
     * Enables physically sorting records within each written data file according to the target
     * table's {@link org.apache.iceberg.SortOrder}.
     *
     * <p>By default, {@link WriteRows} does not sort records unless {@link DistributionMode#RANGE}
     * is configured, so existing pipelines writing to tables that have a metadata sort order incur
     * no additional sorting or shuffle overhead.
     */
    public WriteRows withSortedWrites() {
      return withSortedWrites(true);
    }

    /**
     * Sets whether to physically sort records within each written data file according to the target
     * table's {@link org.apache.iceberg.SortOrder}.
     */
    public WriteRows withSortedWrites(boolean sortedWrites) {
      return toBuilder().setSortedWrites(sortedWrites).build();
    }

    /**
     * Defines properties to be passed to the Iceberg writer itself. Note that these properties are
     * execution-scoped, meaning that they are applied to a preexisting table and will not mutate
     * any table-level properties.
     *
     * <p>To set table-level properties that will be applied to dynamically created tables, use the
     * managed Iceberg transform instead, setting the `table_properties` config property.
     *
     * <p>See: https://iceberg.apache.org/docs/latest/configuration/#write-properties
     */
    public WriteRows withWriteProperties(Map<String, String> writeProperties) {
      return toBuilder().setWriteProperties(writeProperties).build();
    }

    /**
     * Defines the desired Partition Spec to be applied when the Iceberg table must be dynamically
     * created, e.g. `bucket(id_field, 32)` or `day(timestamp_field)`
     *
     * <p>See: https://iceberg.apache.org/spec/#partitioning
     */
    public WriteRows withPartitionFields(List<String> partitionFields) {
      return toBuilder().setPartitionFields(partitionFields).build();
    }

    /**
     * Defines the desired Sort Order to be applied when the Iceberg table must be dynamically
     * created, e.g. `int_field desc` or `bucket(modulo_5, 4) asc nulls last`
     *
     * <p>See: https://iceberg.apache.org/spec/#sorting
     */
    public WriteRows withSortOrder(List<String> sortFields) {
      return toBuilder().setSortFields(sortFields).build();
    }

    /**
     * Enables expirable side-input caching of Iceberg table metadata across workers.
     *
     * <p>When enabled, a driver transform periodically polls the Iceberg catalog and broadcasts
     * lightweight table specifications as a side input. Workers construct in-memory {@link Table}
     * representations without issuing remote catalog RPCs, drastically reducing catalog load.
     */
    public WriteRows withSideInputTableCache() {
      return toBuilder().setUseSideInputTableCache(true).build();
    }

    /**
     * Sets the maximum number of distinct table metadata specifications to broadcast in the
     * side-input cache. Any tables exceeding this limit fall back to worker-local catalog loading.
     *
     * <p><b>Note:</b> This option is only supported for bounded (batch) pipelines. Calling this on
     * an unbounded streaming pipeline will throw an exception at pipeline construction.
     */
    public WriteRows withMaximumTableCacheSize(int maximumTableCacheSize) {
      Preconditions.checkArgument(
          maximumTableCacheSize > 0, "maximumTableCacheSize must be greater than 0");
      return toBuilder().setMaximumTableCacheSize(maximumTableCacheSize).build();
    }

    /**
     * Sets the interval at which table metadata is refreshed from the Iceberg catalog.
     *
     * <p>Applicable for unbounded streaming pipelines. Defaults to 5 minutes.
     */
    public WriteRows withTableCacheRefreshInterval(Duration refreshInterval) {
      Preconditions.checkNotNull(refreshInterval, "refreshInterval must not be null");
      Preconditions.checkArgument(
          refreshInterval.isLongerThan(Duration.ZERO), "refreshInterval must be greater than 0");
      return toBuilder().setTableCacheRefreshInterval(refreshInterval).build();
    }

    /**
     * Sets the number of parallel buckets/workers used to query the Iceberg catalog during
     * refreshes. Defaults to 1 to serialize catalog queries and protect catalogs from connection
     * spikes.
     */
    public WriteRows withTableCachePollingBuckets(int pollingBuckets) {
      Preconditions.checkArgument(
          pollingBuckets > 0, "tableCachePollingBuckets must be greater than 0");
      return toBuilder().setTableCachePollingBuckets(pollingBuckets).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(
          DisplayData.item("distributionMode", getDistributionMode().modeName())
              .withLabel("Distribution Mode"));
      builder.add(
          DisplayData.item("autoSharding", getAutoSharding()).withLabel("Using Auto-Sharding"));
      builder.add(
          DisplayData.item(
                  "sortedWrites",
                  getSortedWrites() || getDistributionMode() == DistributionMode.RANGE)
              .withLabel("Using Sorted Writes"));
      builder.add(
          DisplayData.item("useSideInputTableCache", getUseSideInputTableCache())
              .withLabel("Using Side-Input Table Cache"));
      builder.addIfNotNull(
          DisplayData.item("maximumTableCacheSize", getMaximumTableCacheSize())
              .withLabel("Maximum Cache Size"));
      builder.addIfNotNull(
          DisplayData.item("tableCacheRefreshInterval", getTableCacheRefreshInterval())
              .withLabel("Table Refresh Interval"));
      builder.addIfNotNull(
          DisplayData.item("tableCachePollingBuckets", getTableCachePollingBuckets())
              .withLabel("Catalog Polling Buckets"));
    }

    @Override
    public IcebergWriteResult expand(PCollection<Row> input) {
      List<?> allToArgs = Arrays.asList(getTableIdentifier(), getDynamicDestinations());
      Preconditions.checkArgument(
          1 == allToArgs.stream().filter(Predicates.notNull()).count(),
          "Must set exactly one of table identifier or dynamic destinations object.");

      DynamicDestinations destinations = getDynamicDestinations();
      if (destinations == null) {
        destinations =
            DynamicDestinations.singleTable(
                Preconditions.checkNotNull(getTableIdentifier()),
                input.getSchema(),
                getPartitionFields(),
                getSortFields());
      }

      // Assign destinations before re-windowing to global in WriteToDestinations because
      // user's dynamic destination may depend on windowing properties
      if (IcebergUtils.validDirectWriteLimit(getDirectWriteByteLimit())) {
        Preconditions.checkArgument(
            IcebergUtils.isUnbounded(input),
            "Must only provide direct write limit for unbounded pipelines.");
      }

      boolean hasSideInputOptions =
          getMaximumTableCacheSize() != null
              || getTableCacheRefreshInterval() != null
              || getTableCachePollingBuckets() != null;
      Preconditions.checkArgument(
          getUseSideInputTableCache() || !hasSideInputOptions,
          "Cannot specify side-input cache sub-options (maximumTableCacheSize, "
              + "tableCacheRefreshInterval, tableCachePollingBuckets) without enabling side-input table cache via withSideInputTableCache().");

      PCollectionView<Map<String, SerializableTableSpec>> metadataView = null;
      if (getUseSideInputTableCache()) {
        TableMetadataDriver.Builder driverBuilder =
            TableMetadataDriver.builder()
                .setCatalogConfig(getCatalogConfig())
                .setDynamicDestinations(destinations);

        if (getMaximumTableCacheSize() != null) {
          driverBuilder.setMaximumCacheSize(getMaximumTableCacheSize());
        }
        if (getTableCacheRefreshInterval() != null) {
          driverBuilder.setRefreshInterval(getTableCacheRefreshInterval());
        }
        if (getTableCachePollingBuckets() != null) {
          driverBuilder.setPollingBuckets(getTableCachePollingBuckets());
        }

        metadataView = input.apply("GenerateTableMetadataView", driverBuilder.build().asView());
      }

      boolean sortedWrites = getSortedWrites() || getDistributionMode() == DistributionMode.RANGE;
      Preconditions.checkArgument(
          !getAutoSharding() || getDistributionMode() == DistributionMode.HASH,
          "Autosharding option is only available with 'hash' distribution mode.");
      Preconditions.checkArgument(
          getDistributionFunction() == null || getDistributionMode() == DistributionMode.RANGE,
          "Distribution function is only available with 'range' distribution mode.");

      switch (getDistributionMode()) {
        case NONE:
          return input
              .apply("Assign Table Destinations", new AssignDestinations(destinations))
              .apply(
                  "Write Rows to Destinations",
                  new WriteToDestinations(
                      getCatalogConfig(),
                      destinations,
                      getTriggeringFrequency(),
                      getDirectWriteByteLimit(),
                      getWriteProperties(),
                      metadataView,
                      sortedWrites));
        case HASH:
        case RANGE:
          String assignStepName =
              getDistributionMode() == DistributionMode.RANGE
                  ? "AssignDestinationAndPartitionWithRange"
                  : "AssignDestinationAndPartition";
          return input
              .apply(
                  assignStepName,
                  new AssignDestinationsAndPartitions(
                      destinations,
                      getCatalogConfig(),
                      metadataView,
                      getDistributionMode(),
                      getDistributionFunction()))
              .apply(
                  "Write Rows to Partitions",
                  new WriteToPartitions(
                      getCatalogConfig(),
                      destinations,
                      getTriggeringFrequency(),
                      getAutoSharding(),
                      getWriteProperties(),
                      metadataView,
                      sortedWrites));
        default:
          throw new UnsupportedOperationException(
              "Unsupported distribution mode: " + getDistributionMode());
      }
    }
  }

  public static ReadRows readRows(IcebergCatalogConfig catalogConfig) {
    return new AutoValue_IcebergIO_ReadRows.Builder()
        .setCatalogConfig(catalogConfig)
        .setUseCdc(false)
        .setMetadataColumns(ImmutableList.of())
        .build();
  }

  @AutoValue
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {
    public enum StartingStrategy {
      EARLIEST,
      LATEST
    }

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract boolean getUseCdc();

    abstract @Nullable Long getFromSnapshot();

    abstract @Nullable Long getToSnapshot();

    abstract @Nullable Long getFromTimestamp();

    abstract @Nullable Long getToTimestamp();

    abstract @Nullable StartingStrategy getStartingStrategy();

    abstract @Nullable Boolean getStreaming();

    abstract @Nullable Duration getPollInterval();

    abstract @Nullable List<String> getKeep();

    abstract @Nullable List<String> getDrop();

    abstract @Nullable String getFilter();

    abstract @Nullable String getWatermarkColumn();

    abstract @Nullable String getWatermarkColumnTimeUnit();

    abstract List<String> getMetadataColumns();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract Builder setUseCdc(boolean useCdc);

      abstract Builder setFromSnapshot(@Nullable Long fromSnapshot);

      abstract Builder setToSnapshot(@Nullable Long toSnapshot);

      abstract Builder setFromTimestamp(@Nullable Long fromTimestamp);

      abstract Builder setToTimestamp(@Nullable Long toTimestamp);

      abstract Builder setStartingStrategy(@Nullable StartingStrategy strategy);

      abstract Builder setStreaming(@Nullable Boolean streaming);

      abstract Builder setPollInterval(@Nullable Duration triggeringFrequency);

      abstract Builder setKeep(@Nullable List<String> fields);

      abstract Builder setDrop(@Nullable List<String> fields);

      abstract Builder setFilter(@Nullable String filter);

      abstract Builder setWatermarkColumn(@Nullable String watermarkColumn);

      abstract Builder setWatermarkColumnTimeUnit(@Nullable String timeUnit);

      abstract Builder setMetadataColumns(List<String> metadataColumns);

      abstract ReadRows build();
    }

    public ReadRows withCdc() {
      return toBuilder().setUseCdc(true).build();
    }

    public ReadRows from(TableIdentifier tableIdentifier) {
      return toBuilder().setTableIdentifier(tableIdentifier).build();
    }

    public ReadRows fromSnapshot(@Nullable Long fromSnapshot) {
      return toBuilder().setFromSnapshot(fromSnapshot).build();
    }

    public ReadRows toSnapshot(@Nullable Long toSnapshot) {
      return toBuilder().setToSnapshot(toSnapshot).build();
    }

    public ReadRows fromTimestamp(@Nullable Long fromTimestamp) {
      return toBuilder().setFromTimestamp(fromTimestamp).build();
    }

    public ReadRows toTimestamp(@Nullable Long toTimestamp) {
      return toBuilder().setToTimestamp(toTimestamp).build();
    }

    public ReadRows withPollInterval(Duration pollInterval) {
      return toBuilder().setPollInterval(pollInterval).build();
    }

    public ReadRows streaming(@Nullable Boolean streaming) {
      return toBuilder().setStreaming(streaming).build();
    }

    public ReadRows withStartingStrategy(@Nullable StartingStrategy strategy) {
      return toBuilder().setStartingStrategy(strategy).build();
    }

    public ReadRows keeping(@Nullable List<String> keep) {
      return toBuilder().setKeep(keep).build();
    }

    public ReadRows dropping(@Nullable List<String> drop) {
      return toBuilder().setDrop(drop).build();
    }

    public ReadRows withFilter(@Nullable String filter) {
      return toBuilder().setFilter(filter).build();
    }

    public ReadRows withWatermarkColumn(@Nullable String watermarkColumn) {
      return toBuilder().setWatermarkColumn(watermarkColumn).build();
    }

    public ReadRows withWatermarkColumnTimeUnit(@Nullable String timeUnit) {
      return toBuilder().setWatermarkColumnTimeUnit(timeUnit).build();
    }

    /**
     * Appends top-level metadata columns to CDC output rows.
     *
     * <p>Supported values are {@code _change_type}, {@code _commit_snapshot_id}, {@code
     * _commit_snapshot_sequence_number}, {@code _row_id}, and {@code
     * _last_updated_sequence_number}. The row metadata columns are read from Iceberg data files and
     * require a row-lineage table. The changelog metadata columns come from the emitted change kind
     * and snapshot context and are appended when final Beam rows are emitted.
     *
     * <p>This option is only valid {@link #withCdc()}.
     */
    public ReadRows withMetadataColumns(@Nullable List<String> metadataColumns) {
      return toBuilder()
          .setMetadataColumns(metadataColumns == null ? ImmutableList.of() : metadataColumns)
          .build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      TableIdentifier tableId =
          checkStateNotNull(getTableIdentifier(), "Must set a table to read from.");

      Table table = TableCache.get(getCatalogConfig(), tableId);

      @Nullable String updateCompatibilityVersion =
          input
              .getPipeline()
              .getOptions()
              .as(StreamingOptions.class)
              .getUpdateCompatibilityVersion();

      IcebergScanConfig scanConfig =
          IcebergScanConfig.builder()
              .setCatalogConfig(getCatalogConfig())
              .setScanType(IcebergScanConfig.ScanType.TABLE)
              .setTableIdentifier(tableId)
              .setSchema(
                  IcebergUtils.icebergSchemaToBeamSchema(
                      table.schema(), updateCompatibilityVersion))
              .setUpdateCompatibilityVersion(updateCompatibilityVersion)
              .setFromSnapshotInclusive(getFromSnapshot())
              .setToSnapshot(getToSnapshot())
              .setFromTimestamp(getFromTimestamp())
              .setToTimestamp(getToTimestamp())
              .setStartingStrategy(getStartingStrategy())
              .setStreaming(getStreaming())
              .setPollInterval(getPollInterval())
              .setUseCdc(getUseCdc())
              .setKeepFields(getKeep())
              .setDropFields(getDrop())
              .setFilterString(getFilter())
              .setWatermarkColumn(getWatermarkColumn())
              .setWatermarkColumnTimeUnit(getWatermarkColumnTimeUnit())
              .setMetadataColumns(getMetadataColumns())
              .build();
      scanConfig.validate(table);

      PTransform<PBegin, PCollection<Row>> source =
          getUseCdc()
              ? new IncrementalChangelogSource(scanConfig)
              : Read.from(new ScanSource(scanConfig));

      return input.apply(source);
    }
  }
}
