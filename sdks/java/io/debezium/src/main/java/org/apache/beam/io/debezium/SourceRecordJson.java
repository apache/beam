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
package org.apache.beam.io.debezium;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.GsonBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.annotations.SerializedName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class can be used as a mapper for each {@link SourceRecord} retrieved.
 *
 * <h3>What it does</h3>
 *
 * <p>It maps any SourceRecord retrieved from any supported {@link io.debezium.connector} to JSON
 *
 * <h3>How it works</h3>
 *
 * <p>It will extract valuable fields from any given SourceRecord:
 *
 * <ul>
 *   <li>before - {@link #loadBefore}
 *   <li>after - {@link #loadAfter}
 *   <li>metadata - {@link #loadMetadata}
 *       <ul>
 *         <li>connector - Connector used
 *         <li>version - Connector version
 *         <li>name - Connector name
 *         <li>database - Database name
 *         <li>schema - Database Schema (for MySQL, the binlog file name)
 *         <li>table - Table name
 *       </ul>
 *   <li>transaction - {@link #loadTransaction}: the Debezium {@code transaction} block of the
 *       change event ({@code id}, {@code total_order} and {@code data_collection_order}). The key
 *       is absent unless the connector has {@code provide.transaction.metadata=true}
 * </ul>
 *
 * <h3>Full metadata</h3>
 *
 * <p>When constructed with {@code fullMetadata = true} (see {@link
 * SourceRecordJsonMapper#SourceRecordJsonMapper(boolean)} and {@link
 * DebeziumIO.Read#withFullMetadata(boolean)}), extra keys are appended to {@code metadata} after
 * {@code table}:
 *
 * <ul>
 *   <li>op - The Debezium operation ({@code c}, {@code u}, {@code d}, {@code r}, {@code t}), if the
 *       record envelope has an {@code op} field
 *   <li>Every other field of the Debezium {@code source} block, in schema order, that is not
 *       already represented above (for example {@code ts_ms}, {@code snapshot}, MySQL {@code file},
 *       {@code pos}, {@code row}, or PostgreSQL {@code lsn}, {@code txId})
 * </ul>
 *
 * <p>By default ({@code fullMetadata = false}) the output only contains the six keys above.
 *
 * <h3>Transaction metadata</h3>
 *
 * <p>When the connector has {@code provide.transaction.metadata=true}, Debezium also emits a {@code
 * BEGIN} and an {@code END} event around the change events of each transaction. These have no
 * {@code source}, {@code before} or {@code after}, so they are mapped to their own payload instead
 * ({@link #loadTransactionEvent}): {@code status}, {@code id}, {@code ts_ms}, {@code event_count}
 * and {@code data_collections}.
 *
 * <h3>Usage Example</h3>
 *
 * <p>Map each SourceRecord to JSON
 *
 * <pre>
 *     DebeziumIO.read()
 *         .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper()
 * </pre>
 */
@SuppressWarnings({"nullness"})
@SuppressFBWarnings(
    value = "CT_CONSTRUCTOR_THROW",
    justification =
        "Pre-existing finding, not triaged yet."
            + " Making the class final or moving the throwing code"
            + " into a static factory method may fix it.")
public class SourceRecordJson {
  /**
   * Source fields that are already represented in {@link Event.Metadata} (or would collide with an
   * existing metadata key), so they are never added as full metadata extras.
   */
  private static final Set<String> RESERVED_METADATA_KEYS =
      ImmutableSet.of(
          "connector", "version", "name", "db", "database", "schema", "table", "op", "transaction");

  private static final String SCHEMA_TRANSACTION_METADATA_EVENT_KEY_NAME =
      "io.debezium.connector.common.TransactionMetadataKey";

  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  private final @Nullable Struct value;
  private final @Nullable Event event;
  private final @Nullable TransactionEvent transactionEvent;
  private final Map<String, Object> metadataExtras;

  /**
   * Initializer.
   *
   * @param sourceRecord retrieved SourceRecord using a supported SourceConnector
   */
  public SourceRecordJson(@Nullable SourceRecord sourceRecord) {
    this(sourceRecord, false);
  }

  /**
   * Initializer.
   *
   * @param sourceRecord retrieved SourceRecord using a supported SourceConnector
   * @param fullMetadata whether to add {@code op} and the remaining Debezium {@code source} fields
   *     to {@code metadata}
   */
  public SourceRecordJson(@Nullable SourceRecord sourceRecord, boolean fullMetadata) {
    if (sourceRecord == null) {
      throw new IllegalArgumentException();
    }

    this.value = (Struct) sourceRecord.value();

    if (this.value == null) {
      this.event = new Event(null, null, null, null);
      this.transactionEvent = null;
      this.metadataExtras = Collections.emptyMap();
    } else if (isTransactionMetadataEvent(sourceRecord.keySchema())) {
      this.event = null;
      this.transactionEvent = this.loadTransactionEvent();
      this.metadataExtras = Collections.emptyMap();
    } else {
      Event.Metadata metadata = this.loadMetadata();
      Event.Before before = this.loadBefore();
      Event.After after = this.loadAfter();
      Event.Transaction transaction = this.loadTransaction();

      this.event = new Event(metadata, before, after, transaction);
      this.transactionEvent = null;
      this.metadataExtras =
          fullMetadata && metadata != null ? this.loadMetadataExtras() : Collections.emptyMap();
    }
  }

  /**
   * Extracts metadata from the SourceRecord.
   *
   * @return Metadata
   */
  private Event.Metadata loadMetadata() {
    @Nullable Struct source;
    try {
      source = (Struct) this.value.get("source");
    } catch (RuntimeException e) {
      throw new IllegalArgumentException();
    }
    @Nullable String schema;

    if (source == null) {
      return null;
    }

    try {
      // PostgreSQL and SQL server use Schema
      schema = source.getString("schema");
    } catch (DataException e) {
      // MySQL uses file instead
      schema = source.getString("file");
    }

    return new Event.Metadata(
        source.getString("connector"),
        source.getString("version"),
        source.getString("name"),
        source.getString("db"),
        schema,
        source.getString("table"));
  }

  /**
   * Extracts the full metadata extras: the envelope's {@code op} (if the value schema has one),
   * then every field of the Debezium {@code source} block, in schema order, that isn't already
   * represented in {@link Event.Metadata}.
   *
   * @return extra metadata keys, in insertion order
   */
  private Map<String, Object> loadMetadataExtras() {
    Struct source = (Struct) this.value.get("source");
    Map<String, Object> extras = new LinkedHashMap<>();
    if (this.value.schema().field("op") != null) {
      extras.put("op", toJsonFriendly(this.value.get("op")));
    }
    for (Field field : source.schema().fields()) {
      String fieldName = field.name();
      if (!RESERVED_METADATA_KEYS.contains(fieldName)) {
        extras.put(fieldName, toJsonFriendly(source.get(field)));
      }
    }
    return extras;
  }

  private static @Nullable Object toJsonFriendly(@Nullable Object value) {
    // Source fields are flat primitives today; stringify a nested Struct to keep metadata flat.
    return value instanceof Struct ? String.valueOf(value) : value;
  }

  /**
   * Extracts the before field within SourceRecord.
   *
   * @return Before
   */
  private Event.Before loadBefore() {
    @Nullable Struct before;
    try {
      before = (Struct) this.value.get("before");
    } catch (DataException e) {
      return null;
    }
    if (before == null) {
      return null;
    }

    Map<String, Object> fields = new HashMap<>();
    for (Field field : before.schema().fields()) {
      fields.put(field.name(), before.get(field));
    }

    return new Event.Before(fields);
  }

  /**
   * Extracts the after field within SourceRecord.
   *
   * @return After
   */
  private Event.After loadAfter() {
    @Nullable Struct after;
    try {
      after = (Struct) this.value.get("after");
    } catch (DataException e) {
      return null;
    }
    if (after == null) {
      return null;
    }

    Map<String, Object> fields = new HashMap<>();
    for (Field field : after.schema().fields()) {
      fields.put(field.name(), after.get(field));
    }

    return new Event.After(fields);
  }

  /**
   * Extracts the transaction field within SourceRecord, set when the connector has {@code
   * provide.transaction.metadata=true}.
   *
   * @return Transaction
   */
  private Event.Transaction loadTransaction() {
    @Nullable Struct transaction;
    try {
      transaction = (Struct) this.value.get("transaction");
    } catch (DataException e) {
      return null;
    }
    if (transaction == null) {
      return null;
    }

    return new Event.Transaction(
        getStringOrNull(transaction, "id"),
        getInt64OrNull(transaction, "total_order"),
        getInt64OrNull(transaction, "data_collection_order"));
  }

  /**
   * Extracts the payload of a transaction BEGIN/END event.
   *
   * @return TransactionEvent
   */
  private TransactionEvent loadTransactionEvent() {
    @Nullable List<TransactionEvent.DataCollection> dataCollections = null;
    @Nullable List<Struct> dataCollectionStructs = getArrayOrNull(this.value, "data_collections");
    if (dataCollectionStructs != null) {
      dataCollections = new ArrayList<>();
      for (Struct dataCollection : dataCollectionStructs) {
        dataCollections.add(
            new TransactionEvent.DataCollection(
                getStringOrNull(dataCollection, "data_collection"),
                getInt64OrNull(dataCollection, "event_count")));
      }
    }

    return new TransactionEvent(
        getStringOrNull(this.value, "status"),
        getStringOrNull(this.value, "id"),
        getInt64OrNull(this.value, "ts_ms"),
        getInt64OrNull(this.value, "event_count"),
        dataCollections);
  }

  /**
   * Transforms the extracted data to a JSON string.
   *
   * @return JSON String
   */
  public String toJson() {
    if (this.transactionEvent != null) {
      return this.transactionEvent.toJson();
    }
    return this.event.toJson(this.metadataExtras);
  }

  /** {@link SourceRecordJson} implementation. */
  public static class SourceRecordJsonMapper implements SourceRecordMapper<String> {
    private final boolean fullMetadata;

    /** Creates a mapper that emits the default metadata. */
    public SourceRecordJsonMapper() {
      this(false);
    }

    /**
     * Creates a mapper.
     *
     * @param fullMetadata whether to add {@code op} and the remaining Debezium {@code source}
     *     fields to {@code metadata}
     */
    public SourceRecordJsonMapper(boolean fullMetadata) {
      this.fullMetadata = fullMetadata;
    }

    @Override
    public String mapSourceRecord(SourceRecord sourceRecord) throws Exception {
      return new SourceRecordJson(sourceRecord, fullMetadata).toJson();
    }
  }

  /** Depicts a SourceRecord as an Event in order for it to be mapped as JSON. */
  @SuppressWarnings("unused") // Unused params are validated as JSON
  static class Event implements Serializable {
    private final SourceRecordJson.Event.Metadata metadata;
    private final SourceRecordJson.Event.Before before;
    private final SourceRecordJson.Event.After after;
    private final SourceRecordJson.Event.Transaction transaction;

    /**
     * Event Initializer.
     *
     * @param metadata Metadata retrieved from SourceRecord
     * @param before Before data retrieved from SourceRecord
     * @param after After data retrieved from SourceRecord
     * @param transaction Transaction block retrieved from SourceRecord
     */
    public Event(
        SourceRecordJson.Event.Metadata metadata,
        SourceRecordJson.Event.Before before,
        SourceRecordJson.Event.After after,
        SourceRecordJson.Event.Transaction transaction) {
      this.metadata = metadata;
      this.before = before;
      this.after = after;
      this.transaction = transaction;
    }

    /**
     * Transforms the Event to a JSON string.
     *
     * @return JSON String
     */
    public String toJson() {
      return toJson(Collections.emptyMap());
    }

    /**
     * Transforms the Event to a JSON string, appending the given extra keys to {@code metadata}
     * after {@code table}. The {@code transaction} key is omitted when the record has none.
     *
     * @param extras Additional metadata keys, in iteration order
     * @return JSON String
     */
    String toJson(Map<String, Object> extras) {
      JsonObject tree = GSON.toJsonTree(this).getAsJsonObject();
      if (transaction == null) {
        tree.remove("transaction");
      }
      if (extras.isEmpty() || metadata == null) {
        return GSON.toJson(tree);
      }
      // Gson can't flatten a map into an object, so append the extras to the metadata tree.
      JsonObject metadataJson = tree.getAsJsonObject("metadata");
      for (Map.Entry<String, Object> extra : extras.entrySet()) {
        metadataJson.add(extra.getKey(), GSON.toJsonTree(extra.getValue()));
      }
      return GSON.toJson(tree);
    }

    /** Depicts the metadata within a SourceRecord. It has valuable fields. */
    @SuppressWarnings("unused") // Unused params are validated as JSON
    static class Metadata implements Serializable {
      private final @Nullable String connector;
      private final @Nullable String version;
      private final @Nullable String name;
      private final @Nullable String database;
      private final @Nullable String schema;
      private final @Nullable String table;

      /**
       * Metadata Initializer.
       *
       * @param connector Connector used
       * @param version Connector version
       * @param name Connector name
       * @param database DB name
       * @param schema Schema name
       * @param table Table name
       */
      public Metadata(
          @Nullable String connector,
          @Nullable String version,
          @Nullable String name,
          @Nullable String database,
          @Nullable String schema,
          @Nullable String table) {
        this.connector = connector;
        this.version = version;
        this.name = name;
        this.database = database;
        this.schema = schema;
        this.table = table;
      }
    }

    /** Depicts the before field within SourceRecord. */
    static class Before implements Serializable {
      private final @Nullable Map<String, Object> fields;

      /**
       * Before Initializer.
       *
       * @param fields Key - Value map with information within Before
       */
      public Before(@Nullable Map<String, Object> fields) {
        this.fields = fields;
      }
    }

    /** Depicts the after field within SourceRecord. */
    static class After implements Serializable {
      private final @Nullable Map<String, Object> fields;

      /**
       * After Initializer.
       *
       * @param fields Key - Value map with information within After
       */
      public After(@Nullable Map<String, Object> fields) {
        this.fields = fields;
      }
    }

    /** Depicts the transaction field within SourceRecord. */
    @SuppressWarnings("unused") // Unused params are validated as JSON
    static class Transaction implements Serializable {
      private final @Nullable String id;

      @SerializedName("total_order")
      private final @Nullable Long totalOrder;

      @SerializedName("data_collection_order")
      private final @Nullable Long dataCollectionOrder;

      /**
       * Transaction Initializer.
       *
       * @param id Transaction id
       * @param totalOrder Position of the event among all events of the transaction
       * @param dataCollectionOrder Position of the event among the events of its data collection
       */
      public Transaction(
          @Nullable String id, @Nullable Long totalOrder, @Nullable Long dataCollectionOrder) {
        this.id = id;
        this.totalOrder = totalOrder;
        this.dataCollectionOrder = dataCollectionOrder;
      }
    }
  }

  /** Depicts a transaction BEGIN/END SourceRecord as a TransactionEvent to be mapped as JSON. */
  @SuppressWarnings("unused") // Unused params are validated as JSON
  static class TransactionEvent implements Serializable {
    private final @Nullable String status;
    private final @Nullable String id;

    @SerializedName("ts_ms")
    private final @Nullable Long tsMs;

    @SerializedName("event_count")
    private final @Nullable Long eventCount;

    @SerializedName("data_collections")
    private final @Nullable List<DataCollection> dataCollections;

    /**
     * TransactionEvent Initializer.
     *
     * @param status {@code BEGIN} or {@code END}
     * @param id Transaction id
     * @param tsMs Time the transaction began or ended, in milliseconds
     * @param eventCount Number of events in the transaction, only set on {@code END}
     * @param dataCollections Number of events per data collection, only set on {@code END}
     */
    public TransactionEvent(
        @Nullable String status,
        @Nullable String id,
        @Nullable Long tsMs,
        @Nullable Long eventCount,
        @Nullable List<DataCollection> dataCollections) {
      this.status = status;
      this.id = id;
      this.tsMs = tsMs;
      this.eventCount = eventCount;
      this.dataCollections = dataCollections;
    }

    /**
     * Transforms the TransactionEvent to a JSON string.
     *
     * @return JSON String
     */
    public String toJson() {
      return GSON.toJson(this);
    }

    /** Depicts an entry of the data_collections field within SourceRecord. */
    @SuppressWarnings("unused") // Unused params are validated as JSON
    static class DataCollection implements Serializable {
      @SerializedName("data_collection")
      private final @Nullable String dataCollection;

      @SerializedName("event_count")
      private final @Nullable Long eventCount;

      /**
       * DataCollection Initializer.
       *
       * @param dataCollection Data collection (table) name
       * @param eventCount Number of events of the transaction in this data collection
       */
      public DataCollection(@Nullable String dataCollection, @Nullable Long eventCount) {
        this.dataCollection = dataCollection;
        this.eventCount = eventCount;
      }
    }
  }

  /**
   * Whether the record is a transaction BEGIN/END event, emitted when {@code
   * provide.transaction.metadata} is enabled. These are identified by their key schema name.
   */
  private static boolean isTransactionMetadataEvent(@Nullable Schema keySchema) {
    return keySchema != null && SCHEMA_TRANSACTION_METADATA_EVENT_KEY_NAME.equals(keySchema.name());
  }

  /** Returns the string field's value, or null if the struct's schema has no such field. */
  private static @Nullable String getStringOrNull(Struct s, String name) {
    return s.schema().field(name) != null ? s.getString(name) : null;
  }

  /** Returns the int64 field's value, or null if the struct's schema has no such field. */
  private static @Nullable Long getInt64OrNull(Struct s, String name) {
    return s.schema().field(name) != null ? s.getInt64(name) : null;
  }

  /** Returns the array field's value, or null if the struct's schema has no such field. */
  private static <T> @Nullable List<T> getArrayOrNull(Struct s, String name) {
    return s.schema().field(name) != null ? s.getArray(name) : null;
  }
}
