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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
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
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
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
  // Positions and types of the non-PK data fields in the input row schema, precomputed once so
  // the per-record hash/equals loops need no name lookups. The input schema is fixed by the
  // CoGroupByKey's coder, so positions are stable across elements.
  private final int[] nonPkIndices;
  private final Schema.FieldType[] nonPkTypes;
  private transient @MonotonicNonNull RowResolver resolver;

  ResolveChanges(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    Schema inputSchema =
        CdcOutputUtils.readBeamSchemaWithRowMetadata(
            scanConfig.getMetadataColumns(), scanConfig.getSchema());
    this.rowFilter =
        new RowFilter(inputSchema)
            .keep(
                CdcOutputUtils.readSchemaWithRowMetadata(
                        scanConfig.getMetadataColumns(), scanConfig.getProjectedSchema())
                    .columns().stream()
                    .map(Types.NestedField::name)
                    .collect(Collectors.toList()));
    this.outputSchema =
        CdcOutputUtils.outputSchema(
            scanConfig,
            IcebergUtils.icebergSchemaToBeamSchema(
                scanConfig.getProjectedSchema(), scanConfig.getUpdateCompatibilityVersion()));

    Set<String> pkFields = new HashSet<>(scanConfig.rowIdBeamSchema().getFieldNames());
    List<String> metadataColumns = scanConfig.getMetadataColumns();
    List<Integer> indices = new ArrayList<>();
    List<Schema.FieldType> types = new ArrayList<>();
    List<Schema.Field> fields = inputSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      String name = field.getName();
      if (pkFields.contains(name)
          || (IcebergCdcMetadataColumns.isSupportedColumn(name)
              && metadataColumns.contains(name))) {
        continue;
      }
      indices.add(i);
      types.add(field.getType());
    }
    this.nonPkIndices = indices.stream().mapToInt(Integer::intValue).toArray();
    this.nonPkTypes = types.toArray(new Schema.FieldType[0]);
  }

  @Setup
  public void setup() {
    this.resolver = new RowResolver(nonPkIndices, nonPkTypes);
  }

  @ProcessElement
  public void processElement(
      @Element KV<CdcRowDescriptor, CoGbkResult> element,
      @Timestamp Instant timestamp,
      OutputReceiver<Row> out) {
    CdcRowDescriptor descriptor = element.getKey();
    CoGbkResult result = element.getValue();

    // should be okay to materialize these lists. a PK collision will likely be a handful of records
    // at most
    List<Row> deletes = Lists.newArrayList(result.getAll(DELETES));
    List<Row> inserts = Lists.newArrayList(result.getAll(INSERTS));

    checkStateNotNull(resolver)
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

  /** Resolver specialization over Beam Rows, using precomputed non-PK field positions. */
  private static final class RowResolver extends CdcResolver<Row> {
    private final int[] nonPkIndices;
    private final Schema.FieldType[] nonPkTypes;

    RowResolver(int[] nonPkIndices, Schema.FieldType[] nonPkTypes) {
      this.nonPkIndices = nonPkIndices;
      this.nonPkTypes = nonPkTypes;
    }

    @Override
    protected int nonPkHash(Row element) {
      int hash = 1;
      for (int i = 0; i < nonPkIndices.length; i++) {
        hash =
            31 * hash + Row.Equals.deepHashCode(element.getValue(nonPkIndices[i]), nonPkTypes[i]);
      }
      return hash;
    }

    @Override
    protected boolean nonPkEquals(Row delete, Row insert) {
      // compare non-PK, we already know PK values are equal
      for (int i = 0; i < nonPkIndices.length; i++) {
        int idx = nonPkIndices[i];
        // return early if two values are not equal
        if (!Row.Equals.deepEquals(insert.getValue(idx), delete.getValue(idx), nonPkTypes[i])) {
          return false;
        }
      }
      return true;
    }
  }
}
