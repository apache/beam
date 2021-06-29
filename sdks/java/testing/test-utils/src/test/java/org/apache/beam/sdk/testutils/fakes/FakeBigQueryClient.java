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
package org.apache.beam.sdk.testutils.fakes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testutils.publishing.BigQueryClient;

/**
 * A fake implementation of BigQuery client for testing purposes only.
 *
 * @see BigQueryClient
 */
public class FakeBigQueryClient extends BigQueryClient {

  private Map<String, List<Map<String, ?>>> rowsPerTable;

  public FakeBigQueryClient() {
    super(null, null, null);
    rowsPerTable = new HashMap<>();
  }

  @Override
  public void createTableIfNotExists(String tableName, Map<String, String> schema) {
    // do nothing. Assume the table exists.
  }

  @Override
  public void insertAll(Collection<Map<String, ?>> rows, Map<String, String> schema, String table) {
    rows.forEach(row -> insertRow(row, table));
  }

  @Override
  public void insertRow(Map<String, ?> newRow, String table) {
    List<Map<String, ?>> rows = rowsPerTable.get(table);

    if (rows == null) {
      rows = new ArrayList<>();
      rows.add(newRow);

      rowsPerTable.put(table, rows);
    } else {
      rows.add(newRow);
    }
  }

  public List<Map<String, ?>> getRows(String table) {
    return rowsPerTable.get(table);
  }
}
