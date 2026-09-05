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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergDestination;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Routes every record to one table, dropping the control columns from the written row. */
final class SingleTableDestinations implements DynamicDestinations {

  private final DynamicDestinations delegate;
  private final RowFilter filter;

  private SingleTableDestinations(DynamicDestinations delegate, RowFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
  }

  static SingleTableDestinations of(
      TableIdentifier tableId, Schema inputSchema, CdcWriteConfig config) {
    List<String> controlColumns = new ArrayList<>();
    @Nullable String changeTypeColumn = config.getChangeTypeColumn();
    if (changeTypeColumn != null && inputSchema.hasField(changeTypeColumn)) {
      controlColumns.add(changeTypeColumn);
    }
    if (inputSchema.hasField(config.getSequenceNumberColumn())) {
      controlColumns.add(config.getSequenceNumberColumn());
    }
    RowFilter filter = new RowFilter(inputSchema);
    if (!controlColumns.isEmpty()) {
      filter = filter.drop(controlColumns);
    }
    return new SingleTableDestinations(
        DynamicDestinations.singleTable(tableId, filter.outputSchema()), filter);
  }

  @Override
  public Schema getDataSchema() {
    return filter.outputSchema();
  }

  @Override
  public Row getData(Row element) {
    return filter.filter(element);
  }

  @Override
  public IcebergDestination instantiateDestination(String destination) {
    return delegate.instantiateDestination(destination);
  }

  @Override
  public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
    return delegate.getTableStringIdentifier(element);
  }
}
