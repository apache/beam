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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.MoreObjects.firstNonNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A test table provider for BigQueryRowCountIT. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigQueryTestTableProvider extends BigQueryTableProvider {

  private Map<String, Table> tableSpecMap;
  private Map<String, BeamSqlTable> beamSqlTableMap;

  BigQueryTestTableProvider() {
    super();
    tableSpecMap = new HashMap<>();
    beamSqlTableMap = new HashMap<>();
  }

  void addTable(String name, Table table) {
    tableSpecMap.put(name, table);
  }

  @Override
  public @Nullable Table getTable(String tableName) {
    return tableSpecMap.get(tableName);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    BeamSqlTable t = beamSqlTableMap.get(table.getLocation());
    if (t != null) {
      return t;
    }

    t =
        new BigQueryTestTable(
            table,
            BigQueryUtils.ConversionOptions.builder()
                .setTruncateTimestamps(
                    firstNonNull(table.getProperties().getBoolean("truncateTimestamps"), false)
                        ? BigQueryUtils.ConversionOptions.TruncateTimestamps.TRUNCATE
                        : BigQueryUtils.ConversionOptions.TruncateTimestamps.REJECT)
                .build());
    beamSqlTableMap.put(table.getLocation(), t);

    return t;
  }
}
