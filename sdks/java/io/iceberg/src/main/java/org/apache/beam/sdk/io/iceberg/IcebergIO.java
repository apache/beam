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
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
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
    return new AutoValue_IcebergIO_WriteRows.Builder().setCatalogConfig(catalog).build();
  }

  @AutoValue
  public abstract static class WriteRows extends PTransform<PCollection<Row>, IcebergWriteResult> {

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract @Nullable DynamicDestinations getDynamicDestinations();

    abstract @Nullable Duration getTriggeringFrequency();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract Builder setDynamicDestinations(DynamicDestinations destinations);

      abstract Builder setTriggeringFrequency(Duration triggeringFrequency);

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
                Preconditions.checkNotNull(getTableIdentifier()), input.getSchema());
      }

      // Assign destinations before re-windowing to global in WriteToDestinations because
      // user's dynamic destination may depend on windowing properties
      return input
          .apply("Assign Table Destinations", new AssignDestinations(destinations))
          .apply(
              "Write Rows to Destinations",
              new WriteToDestinations(getCatalogConfig(), destinations, getTriggeringFrequency()));
    }
  }

  public static ReadRows readRows(IcebergCatalogConfig catalogConfig) {
    return new AutoValue_IcebergIO_ReadRows.Builder()
        .setCatalogConfig(catalogConfig)
        .setUseCdc(false)
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

    @Override
    public PCollection<Row> expand(PBegin input) {
      TableIdentifier tableId =
          checkStateNotNull(getTableIdentifier(), "Must set a table to read from.");

      Table table = getCatalogConfig().catalog().loadTable(tableId);

      IcebergScanConfig scanConfig =
          IcebergScanConfig.builder()
              .setCatalogConfig(getCatalogConfig())
              .setScanType(IcebergScanConfig.ScanType.TABLE)
              .setTableIdentifier(tableId)
              .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
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
              .build();
      scanConfig.validate(table);

      PTransform<PBegin, PCollection<Row>> source =
          getUseCdc()
              ? new IncrementalScanSource(scanConfig)
              : Read.from(new ScanSource(scanConfig));

      return input.apply(source);
    }
  }
}
