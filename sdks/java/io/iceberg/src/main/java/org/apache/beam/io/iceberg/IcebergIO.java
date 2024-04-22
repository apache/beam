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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IcebergIO {

  public static WriteRows writeToDynamicDestinations(
      IcebergCatalogConfig catalog, DynamicDestinations dynamicDestinations) {
    return new WriteRows(catalog, dynamicDestinations);
  }

  public static ReadTable readTable(IcebergCatalogConfig catalogConfig, TableIdentifier tableId) {
    return new ReadTable(catalogConfig, tableId);
  }

  static class WriteRows extends PTransform<PCollection<Row>, IcebergWriteResult> {

    private final IcebergCatalogConfig catalog;
    private final DynamicDestinations dynamicDestinations;

    private WriteRows(IcebergCatalogConfig catalog, DynamicDestinations dynamicDestinations) {
      this.catalog = catalog;
      this.dynamicDestinations = dynamicDestinations;
    }

    @Override
    public IcebergWriteResult expand(PCollection<Row> input) {

      return input
          .apply("Set Destination Metadata", new AssignDestinations(dynamicDestinations))
          .apply(
              "Write Rows to Destinations", new WriteToDestinations(catalog, dynamicDestinations));
    }
  }

  public static class ReadTable extends PTransform<PBegin, PCollection<Row>> {

    private final IcebergCatalogConfig catalogConfig;
    private final transient @Nullable TableIdentifier tableId;

    private TableIdentifier getTableId() {
      return checkStateNotNull(
          tableId, "Transient field tableId null; it should not be accessed after serialization");
    }

    private ReadTable(IcebergCatalogConfig catalogConfig, TableIdentifier tableId) {
      this.catalogConfig = catalogConfig;
      this.tableId = tableId;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {

      Table table = catalogConfig.catalog().loadTable(getTableId());

      return input.apply(
          Read.from(
              new ScanSource(
                  IcebergScanConfig.builder()
                      .setCatalogConfig(catalogConfig)
                      .setScanType(IcebergScanConfig.ScanType.TABLE)
                      .setTableIdentifier(getTableId())
                      .setSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(table.schema()))
                      .build())));
    }
  }
}
