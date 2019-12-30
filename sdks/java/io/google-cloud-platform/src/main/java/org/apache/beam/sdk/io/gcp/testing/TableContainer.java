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
import com.google.api.services.bigquery.model.TableRow;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

/** Encapsulates a BigQuery Table, and it's contents. */
class TableContainer {
  Table table;
  List<TableRow> rows;
  List<String> ids;
  Long sizeBytes;

  TableContainer(Table table) {
    this.table = table;

    this.rows = new ArrayList<>();
    this.ids = new ArrayList<>();
    this.sizeBytes = 0L;
  }

  long addRow(TableRow row, String id) {
    rows.add(row);
    ids.add(id);
    long tableSize = table.getNumBytes() == null ? 0L : table.getNumBytes();
    try {
      long rowSize = TableRowJsonCoder.of().getEncodedElementByteSize(row);
      table.setNumBytes(tableSize + rowSize);
      return rowSize;
    } catch (Exception ex) {
      throw new RuntimeException("Failed to convert the row to JSON", ex);
    }
  }

  Table getTable() {
    return table;
  }

  List<TableRow> getRows() {
    return rows;
  }
}
