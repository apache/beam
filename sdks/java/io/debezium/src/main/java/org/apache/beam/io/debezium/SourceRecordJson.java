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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.GsonBuilder;
import org.apache.kafka.connect.data.Field;
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
 *         <li>schema - Database Schema
 *         <li>connector - Connector used
 *         <li>version - Connector version
 *       </ul>
 * </ul>
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
public class SourceRecordJson {
  private final @Nullable Struct value;
  private final @Nullable Event event;

  /**
   * Initializer.
   *
   * @param sourceRecord retrieved SourceRecord using a supported SourceConnector
   */
  public SourceRecordJson(@Nullable SourceRecord sourceRecord) {
    if (sourceRecord == null) {
      throw new IllegalArgumentException();
    }

    this.value = (Struct) sourceRecord.value();

    if (this.value == null) {
      this.event = new Event(null, null, null);
    } else {
      Event.Metadata metadata = this.loadMetadata();
      Event.Before before = this.loadBefore();
      Event.After after = this.loadAfter();

      this.event = new Event(metadata, before, after);
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
   * Transforms the extracted data to a JSON string.
   *
   * @return JSON String
   */
  public String toJson() {
    return this.event.toJson();
  }

  /** {@link SourceRecordJson} implementation. */
  public static class SourceRecordJsonMapper implements SourceRecordMapper<String> {
    @Override
    public String mapSourceRecord(SourceRecord sourceRecord) throws Exception {
      return new SourceRecordJson(sourceRecord).toJson();
    }
  }

  /** Depicts a SourceRecord as an Event in order for it to be mapped as JSON. */
  @SuppressWarnings("unused") // Unused params are validated as JSON
  static class Event implements Serializable {
    private final SourceRecordJson.Event.Metadata metadata;
    private final SourceRecordJson.Event.Before before;
    private final SourceRecordJson.Event.After after;

    /**
     * Event Initializer.
     *
     * @param metadata Metadata retrieved from SourceRecord
     * @param before Before data retrieved from SourceRecord
     * @param after After data retrieved from SourceRecord
     */
    public Event(
        SourceRecordJson.Event.Metadata metadata,
        SourceRecordJson.Event.Before before,
        SourceRecordJson.Event.After after) {
      this.metadata = metadata;
      this.before = before;
      this.after = after;
    }

    /**
     * Transforms the Event to a JSON string.
     *
     * @return JSON String
     */
    public String toJson() {
      Gson gson = new GsonBuilder().serializeNulls().create();
      return gson.toJson(this);
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
  }
}
