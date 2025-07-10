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
package org.apache.beam.sdk.io.gcp.testing;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableRow;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Encapsulates a BigQuery Table, and it's contents. */
class TableContainer {
  Table table;

  @Nullable List<String> primaryKeyColumns = null;
  @Nullable List<Integer> primaryKeyColumnIndices = null;
  Map<List<Object>, Long> lastSequenceNumber = Maps.newHashMap();
  List<TableRow> rows;

  Map<List<Object>, TableRow> keyedRows;
  List<String> ids;
  Long sizeBytes;

  TableContainer(Table table) {
    this.table = table;

    this.rows = new ArrayList<>();
    this.keyedRows = Maps.newHashMap();
    this.ids = new ArrayList<>();
    this.sizeBytes = 0L;
    // extract primary key information from Table if present
    List<String> pkColumns = primaryKeyColumns(table);
    this.primaryKeyColumns = pkColumns;
    this.primaryKeyColumnIndices = primaryColumnFieldIndices(pkColumns, table);
  }

  static @Nullable List<String> primaryKeyColumns(Table table) {
    return Optional.ofNullable(table.getTableConstraints())
        .flatMap(constraints -> Optional.ofNullable(constraints.getPrimaryKey()))
        .map(TableConstraints.PrimaryKey::getColumns)
        .orElse(null);
  }

  static @Nullable List<Integer> primaryColumnFieldIndices(
      @Nullable List<String> primaryKeyColumns, Table table) {
    if (primaryKeyColumns == null) {
      return null;
    }
    Map<String, Integer> indices =
        IntStream.range(0, table.getSchema().getFields().size())
            .boxed()
            .collect(Collectors.toMap(i -> table.getSchema().getFields().get(i).getName(), i -> i));
    List<Integer> primaryKeyColumnIndices = Lists.newArrayList();
    for (String columnName : primaryKeyColumns) {
      primaryKeyColumnIndices.add(Preconditions.checkStateNotNull(indices.get(columnName)));
    }
    return primaryKeyColumnIndices;
  }

  // Only top-level columns supported.
  void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
    this.primaryKeyColumns = primaryKeyColumns;
    this.primaryKeyColumnIndices = primaryColumnFieldIndices(primaryKeyColumns, table);
  }

  @Nullable
  List<Object> getPrimaryKey(TableRow tableRow) {
    if (primaryKeyColumns == null) {
      return null;
    }
    @Nullable Object fValue = tableRow.get("f");
    if (fValue instanceof List) {
      List<Object> cellValues =
          ((List<AbstractMap<String, Object>>) fValue)
              .stream()
                  .map(cell -> Preconditions.checkStateNotNull(cell.get("v")))
                  .collect(Collectors.toList());

      return Preconditions.checkStateNotNull(primaryKeyColumnIndices).stream()
          .map(cellValues::get)
          .collect(Collectors.toList());
    } else {
      return primaryKeyColumns.stream().map(tableRow::get).collect(Collectors.toList());
    }
  }

  long addRow(TableRow row, String id) {
    List<Object> primaryKey = getPrimaryKey(row);
    if (primaryKey != null && !primaryKey.isEmpty()) {
      if (keyedRows.putIfAbsent(primaryKey, row) != null) {
        throw new RuntimeException(
            "Primary key validation error! Multiple inserts with the same primary key.");
      }
    } else {
      rows.add(row);
      if (id != null) {
        ids.add(id);
      }
    }

    long tableSize = table.getNumBytes() == null ? 0L : table.getNumBytes();
    try {
      long rowSize = TableRowJsonCoder.of().getEncodedElementByteSize(row);
      table.setNumBytes(tableSize + rowSize);
      return rowSize;
    } catch (Exception ex) {
      throw new RuntimeException("Failed to convert the row to JSON", ex);
    }
  }

  void upsertRow(TableRow row, long sequenceNumber) {
    List<Object> primaryKey = getPrimaryKey(row);
    if (primaryKey == null) {
      throw new RuntimeException("Upserts only allowed when using primary keys");
    }
    long lastSequenceNumberForKey = lastSequenceNumber.getOrDefault(primaryKey, Long.MIN_VALUE);
    if (sequenceNumber <= lastSequenceNumberForKey) {
      // Out-of-order upsert - ignore it as we've already seen a more-recent update.
      return;
    }

    TableRow oldValue = keyedRows.put(primaryKey, row);
    try {
      long tableSize = table.getNumBytes() == null ? 0L : table.getNumBytes();
      if (oldValue != null) {
        tableSize -= TableRowJsonCoder.of().getEncodedElementByteSize(oldValue);
      }
      tableSize += TableRowJsonCoder.of().getEncodedElementByteSize(row);
      table.setNumBytes(tableSize);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert the row to JSON", e);
    }
    lastSequenceNumber.put(primaryKey, sequenceNumber);
  }

  void deleteRow(TableRow row, long sequenceNumber) {
    List<Object> primaryKey = getPrimaryKey(row);
    if (primaryKey == null) {
      throw new RuntimeException("Upserts only allowed when using primary keys");
    }
    long lastSequenceNumberForKey = lastSequenceNumber.getOrDefault(primaryKey, -1L);
    if (sequenceNumber <= lastSequenceNumberForKey) {
      // Out-of-order upsert - ignore it as we've already seen a more-recent update.
      return;
    }

    TableRow oldValue = keyedRows.remove(primaryKey);
    try {
      if (oldValue != null) {
        long tableSize = table.getNumBytes() == null ? 0L : table.getNumBytes();
        table.setNumBytes(tableSize - TableRowJsonCoder.of().getEncodedElementByteSize(row));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert the row to JSON", e);
    }
    lastSequenceNumber.put(primaryKey, sequenceNumber);
  }

  Table getTable() {
    return table;
  }

  List<TableRow> getRows() {
    if (primaryKeyColumns != null) {
      return Lists.newArrayList(keyedRows.values());
    } else {
      return rows;
    }
  }

  List<String> getIds() {
    return ids;
  }
}
