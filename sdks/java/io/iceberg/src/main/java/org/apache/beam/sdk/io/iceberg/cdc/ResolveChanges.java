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
package org.apache.beam.sdk.io.iceberg.cdc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.joda.time.Instant;

/**
 * Receives a {@link CoGbkResult} containing inserts and deletes sharing the same snapshot sequence
 * number and Primary Key, and uses {@link CdcResolver} to identify logical updates.
 */
class ResolveChanges extends DoFn<KV<CdcRowDescriptor, CoGbkResult>, Row> {
  static final TupleTag<Row> DELETES = new TupleTag<>() {};
  static final TupleTag<Row> INSERTS = new TupleTag<>() {};
  private final IcebergScanConfig scanConfig;
  private final RowFilter rowFilter;
  private final Schema outputSchema;

  ResolveChanges(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    this.rowFilter =
        new RowFilter(
                CdcOutputUtils.readBeamSchemaWithRowMetadata(
                    scanConfig.getMetadataColumns(), scanConfig.getSchema()))
            .keep(
                CdcOutputUtils.readSchemaWithRowMetadata(
                        scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema())
                    .columns().stream()
                    .map(Types.NestedField::name)
                    .collect(Collectors.toList()));
    this.outputSchema =
        CdcOutputUtils.outputSchema(
            scanConfig, IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
  }

  @ProcessElement
  public void processElement(
      @Element KV<CdcRowDescriptor, CoGbkResult> element,
      @Timestamp Instant timestamp,
      OutputReceiver<Row> out) {
    CdcRowDescriptor descriptor = element.getKey();

    Set<String> pkFields = new HashSet<>(descriptor.getPrimaryKey().getSchema().getFieldNames());
    CoGbkResult result = element.getValue();

    // should be okay to materialize these lists. a PK collision will likely be a handful of records
    // at most
    List<Row> deletes = Lists.newArrayList(result.getAll(DELETES));
    List<Row> inserts = Lists.newArrayList(result.getAll(INSERTS));

    new RowResolver(pkFields, scanConfig.getMetadataColumns())
        .resolve(
            deletes,
            inserts,
            (kind, row) -> {
              Row projectedRow = rowFilter.filter(row);
              out.builder(
                      CdcOutputUtils.outputRow(
                          scanConfig.getMetadataColumns(),
                          outputSchema,
                          descriptor.getCommitSnapshotId(),
                          descriptor.getSnapshotSequenceNumber(),
                          kind,
                          projectedRow))
                  .setValueKind(kind)
                  .setTimestamp(timestamp)
                  .output();
            });
  }

  private static final class RowResolver extends CdcResolver<Row> {
    private final Set<String> pkFields;
    private final List<String> metadataColumns;

    RowResolver(Set<String> pkFields, List<String> metadataColumns) {
      this.pkFields = pkFields;
      this.metadataColumns = metadataColumns;
    }

    @Override
    protected int nonPkHash(Row element) {
      int hash = 1;
      for (String field : element.getSchema().getFieldNames()) {
        if (pkFields.contains(field)
            || (IcebergCdcMetadataColumns.isSupportedColumn(field)
                && metadataColumns.contains(field))) {
          continue;
        }
        hash =
            31 * hash
                + Row.Equals.deepHashCode(
                    element.getValue(field), element.getSchema().getField(field).getType());
      }
      return hash;
    }

    @Override
    protected boolean nonPkEquals(Row delete, Row insert) {
      Schema schema = insert.getSchema();
      for (String field : schema.getFieldNames()) {
        // we already know PK values are equal
        if (pkFields.contains(field)
            || (IcebergCdcMetadataColumns.isSupportedColumn(field)
                && metadataColumns.contains(field))) {
          continue;
        }
        // return early if two values are not equal
        if (!Row.Equals.deepEquals(
            insert.getValue(field), delete.getValue(field), schema.getField(field).getType())) {
          return false;
        }
      }
      return true;
    }
  }
}
