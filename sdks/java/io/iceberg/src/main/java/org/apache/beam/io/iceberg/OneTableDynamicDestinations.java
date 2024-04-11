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
package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

class OneTableDynamicDestinations implements DynamicDestinations {

  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Row EMPTY_ROW = Row.nullRow(EMPTY_SCHEMA);

  // TableId represented as String for serializability
  private final String tableIdString;

  private transient @MonotonicNonNull TableIdentifier tableId;

  private TableIdentifier getTableIdentifier() {
    if (tableId == null) {
      tableId = TableIdentifier.parse(tableIdString);
    }
    return tableId;
  }

  OneTableDynamicDestinations(TableIdentifier tableId) {
    this.tableIdString = tableId.toString();
  }

  @Override
  public Schema getMetadataSchema() {
    return EMPTY_SCHEMA;
  }

  @Override
  public Row assignDestinationMetadata(Row data) {
    return EMPTY_ROW;
  }

  @Override
  public IcebergDestination instantiateDestination(Row dest) {
    return IcebergDestination.builder()
        .setTableIdentifier(getTableIdentifier())
        .setTableCreateConfig(null)
        .setFileFormat(FileFormat.PARQUET)
        .build();
  }
}
