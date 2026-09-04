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
package org.apache.beam.sdk.extensions.sql.meta.provider.delta;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

/**
 * {@link TableProvider} for Delta Lake tables.
 *
 * <p>A sample of Delta Lake table registration is:
 *
 * <pre>{@code
 * CREATE EXTERNAL TABLE orders(
 *   id INTEGER,
 *   name VARCHAR,
 *   amount DOUBLE
 * )
 * TYPE 'delta'
 * LOCATION '/path/to/delta/orders'
 * TBLPROPERTIES '{"version": 1}'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class DeltaTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "delta";
  }

  @Override
  public void createTable(Table table) {
    // TODO: Support catalog-based Delta Lake tables once Delta catalog support is implemented.
    checkArgument(
        table.getLocation() != null,
        "Delta Lake table location must be specified (catalog-based tables are not supported).");
    super.createTable(table);
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    // TODO: Support catalog-based Delta Lake tables once Delta catalog support is implemented.
    checkArgument(
        table.getLocation() != null,
        "Delta Lake table location must be specified (catalog-based tables are not supported).");
    return new DeltaTable(table);
  }

  @Override
  public boolean supportsPartitioning(Table table) {
    // TODO: Support partitioning when Delta Lake connector supports partitioned reads.
    return false;
  }
}
