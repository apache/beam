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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteConfiguration.DYNAMIC_DESTINATIONS;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.RowStringInterpolator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
public class PortableBigQueryDestinations
    extends DynamicDestinations<Row, KV<String, @Nullable String>> {
  public static final String DESTINATION = "destination";
  public static final String RECORD = "record";
  public static final String SCHEMA = "schema";

  private static final ConcurrentHashMap<String, TableSchema> JSON_SCHEMA_CACHE =
      new ConcurrentHashMap<>();

  private @MonotonicNonNull RowStringInterpolator interpolator = null;
  private final @Nullable List<String> primaryKey;
  private final RowFilter rowFilter;
  private final @Nullable List<String> clusteringFields;

  public PortableBigQueryDestinations(Schema rowSchema, BigQueryWriteConfiguration configuration) {
    this.clusteringFields = configuration.getClusteringFields();
    // DYNAMIC_DESTINATIONS magic string is the old way of doing it for cross-language.
    // In that case, we do no interpolation
    if (!configuration.getTable().equals(DYNAMIC_DESTINATIONS)) {
      this.interpolator = new RowStringInterpolator(configuration.getTable(), rowSchema);
    }
    this.primaryKey = configuration.getPrimaryKey();
    RowFilter rf = new RowFilter(rowSchema);
    if (configuration.getDrop() != null) {
      rf = rf.drop(configuration.getDrop());
    }
    if (configuration.getKeep() != null) {
      rf = rf.keep(configuration.getKeep());
    }
    if (configuration.getOnly() != null) {
      rf = rf.only(configuration.getOnly());
    }
    this.rowFilter = rf;
  }

  @Override
  public KV<String, @Nullable String> getDestination(@Nullable ValueInSingleWindow<Row> element) {
    if (interpolator != null) {
      return KV.of(interpolator.interpolate(checkArgumentNotNull(element)), null);
    }
    Row row = checkStateNotNull(checkStateNotNull(element).getValue());
    String destination = checkStateNotNull(row.getString(DESTINATION));
    if (row.getSchema().hasField(SCHEMA)) {
      @Nullable String schemaJson = row.getString(SCHEMA);
      return KV.of(destination, schemaJson == null ? "" : schemaJson);
    }
    return KV.of(destination, null);
  }

  @Override
  public Coder<KV<String, @Nullable String>> getDestinationCoder() {
    return KvCoder.of(StringUtf8Coder.of(), NullableCoder.of(StringUtf8Coder.of()));
  }

  @Override
  public TableDestination getTable(KV<String, @Nullable String> destination) {
    String tableSpec = destination.getKey();
    if (clusteringFields != null && !clusteringFields.isEmpty()) {
      Clustering clustering = new Clustering().setFields(clusteringFields);
      return new TableDestination(tableSpec, null, null, clustering);
    }
    return new TableDestination(tableSpec, null);
  }

  @Override
  public @Nullable TableSchema getSchema(KV<String, @Nullable String> destination) {
    @Nullable String schemaJson = destination.getValue();
    if (schemaJson != null) {
      if (schemaJson.isEmpty()) {
        return null;
      }
      return parseTableSchema(schemaJson);
    }
    return BigQueryUtils.toTableSchema(rowFilter.outputSchema());
  }

  @Override
  public @Nullable TableConstraints getTableConstraints(KV<String, @Nullable String> destination) {
    if (primaryKey != null) {
      return new TableConstraints()
          .setPrimaryKey(new TableConstraints.PrimaryKey().setColumns(primaryKey));
    }
    return null;
  }

  private static TableSchema parseTableSchema(String schemaJson) {
    return JSON_SCHEMA_CACHE.computeIfAbsent(
        schemaJson, json -> BigQueryHelpers.fromJsonString(json, TableSchema.class));
  }

  @VisibleForTesting
  static TableRow filterTableRowBySchema(
      Map<String, Object> tableRow, @Nullable List<TableFieldSchema> fields) {
    if (fields == null || fields.isEmpty()) {
      TableRow copy = new TableRow();
      copy.putAll(tableRow);
      return copy;
    }
    TableRow filtered = new TableRow();
    for (TableFieldSchema field : fields) {
      String fieldName = field.getName();
      @Nullable Object value = null;
      if (tableRow.containsKey(fieldName)) {
        value = tableRow.get(fieldName);
      } else {
        for (Map.Entry<String, Object> entry : tableRow.entrySet()) {
          if (entry.getKey().equalsIgnoreCase(fieldName)) {
            value = entry.getValue();
            break;
          }
        }
      }
      if (value == null) {
        continue;
      }
      List<TableFieldSchema> subfields = field.getFields();
      if (subfields != null && !subfields.isEmpty()) {
        if ("REPEATED".equalsIgnoreCase(field.getMode()) && value instanceof Iterable) {
          List<@Nullable Object> filteredList = new ArrayList<>();
          for (Object item : (Iterable<?>) value) {
            if (item instanceof Map) {
              @SuppressWarnings("unchecked")
              Map<String, Object> mapItem = (Map<String, Object>) item;
              filteredList.add(filterTableRowBySchema(mapItem, subfields));
            } else if (item != null) {
              filteredList.add(item);
            }
          }
          value = filteredList;
        } else if (value instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> mapValue = (Map<String, Object>) value;
          value = filterTableRowBySchema(mapValue, subfields);
        }
      }
      filtered.set(fieldName, value);
    }
    return filtered;
  }

  public SerializableFunction<Row, TableRow> getFilterFormatFunction(boolean fetchNestedRecord) {
    return row -> {
      @Nullable String schemaJson = null;
      if (fetchNestedRecord) {
        if (row.getSchema().hasField(SCHEMA)) {
          schemaJson = row.getString(SCHEMA);
        }
        row = checkStateNotNull(row.getRow(RECORD));
      }
      Row filtered = rowFilter.filter(row);
      TableRow tableRow = BigQueryUtils.toTableRow(filtered);
      if (schemaJson != null && !schemaJson.isEmpty()) {
        TableSchema tableSchema = parseTableSchema(schemaJson);
        tableRow = filterTableRowBySchema(tableRow, tableSchema.getFields());
      }
      return tableRow;
    };
  }

  public SerializableFunction<AvroWriteRequest<Row>, GenericRecord> getAvroFilterFormatFunction(
      boolean fetchNestedRecord) {
    return request -> {
      Row row = request.getElement();
      if (fetchNestedRecord) {
        row = checkStateNotNull(row.getRow(RECORD));
      }
      Row filtered = rowFilter.filter(row);
      org.apache.avro.Schema avroSchema = request.getSchema();
      if (avroSchema != null
          && avroSchema.getFields().size() != filtered.getSchema().getFieldCount()) {
        List<String> fieldNames =
            avroSchema.getFields().stream()
                .map(org.apache.avro.Schema.Field::name)
                .collect(Collectors.toList());
        filtered = new RowFilter(filtered.getSchema()).keep(fieldNames).filter(filtered);
      }
      return AvroUtils.toGenericRecord(filtered, avroSchema);
    };
  }
}
