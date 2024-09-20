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
package org.apache.beam.sdk.io.iceberg;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

class OneTableDynamicDestinations implements DynamicDestinations, Externalizable {

  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Row EMPTY_ROW = Row.nullRow(EMPTY_SCHEMA);

  // TableId represented as String for serializability
  private transient @MonotonicNonNull String tableIdString;

  private transient @MonotonicNonNull TableIdentifier tableId;

  @VisibleForTesting
  TableIdentifier getTableIdentifier() {
    if (tableId == null) {
      tableId = TableIdentifier.parse(Preconditions.checkNotNull(tableIdString));
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

  // Need a public default constructor for custom serialization
  public OneTableDynamicDestinations() {}

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(Preconditions.checkNotNull(tableIdString));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    tableIdString = in.readUTF();
    tableId = TableIdentifier.parse(tableIdString);
  }
}
