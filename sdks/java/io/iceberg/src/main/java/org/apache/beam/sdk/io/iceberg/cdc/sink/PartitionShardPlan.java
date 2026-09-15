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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Derives a record's write shard from its Iceberg partition tuple. Each partition owns a block of
 * {@code shards_per_partition} consecutive shards, and the caller's {@code offset} (derived from
 * the primary-key hash) selects one of the shards. Used when {@code shards_per_partition <
 * num_shards}.
 *
 * <p>Correctness rests on one property: this plan exists only under a {@code shards_per_partition}
 * cap below {@code num_shards}, where {@link TableSetup#validatePartitioning} still requires
 * partition source columns to be equality columns, so the shard is a pure function of the primary
 * key and one key's records never split across shards.
 */
final class PartitionShardPlan {

  /** Beam schema of the partition source columns, in the projected Iceberg schema's order. */
  private final Schema sourceSchema;

  /** For each {@link #sourceSchema} field, its position in the CDC data schema. */
  private final int[] sourcePositions;

  /** Iceberg schema of the partition source columns. */
  private final org.apache.iceberg.Schema sourceIcebergSchema;

  /** Adapts a converted record to the internal representation the transforms expect. */
  private final InternalRecordWrapper wrapper;

  /** The spec's bound transforms over a reused partition tuple. */
  private final PartitionKey partitionKey;

  /** Type-aware, JVM-stable hash of the partition tuple. */
  private final JavaHash<StructLike> partitionHash;

  private PartitionShardPlan(
      Schema sourceSchema,
      int[] sourcePositions,
      org.apache.iceberg.Schema sourceIcebergSchema,
      InternalRecordWrapper wrapper,
      PartitionKey partitionKey,
      JavaHash<StructLike> partitionHash) {
    this.sourceSchema = sourceSchema;
    this.sourcePositions = sourcePositions;
    this.sourceIcebergSchema = sourceIcebergSchema;
    this.wrapper = wrapper;
    this.partitionKey = partitionKey;
    this.partitionHash = partitionHash;
  }

  /** Builds the plan for a partitioned spec. Converts only the partition source columns. */
  static PartitionShardPlan of(
      PartitionSpec spec, org.apache.iceberg.Schema tableSchema, Schema cdcDataSchema) {
    // Find distinct source ids since one column can feed several partition fields
    Set<Integer> sourceIds = new LinkedHashSet<>();
    for (PartitionField field : spec.fields()) {
      sourceIds.add(field.sourceId());
    }
    org.apache.iceberg.Schema sourceIcebergSchema = TypeUtil.select(tableSchema, sourceIds);

    List<Types.NestedField> sourceColumns = sourceIcebergSchema.columns();
    Schema.Builder sourceBeamSchemaBuilder = Schema.builder();
    int[] sourcePositions = new int[sourceColumns.size()];
    // convert to a Beam schema using input data schema fields
    for (int i = 0; i < sourceColumns.size(); i++) {
      String name = sourceColumns.get(i).name();
      sourceBeamSchemaBuilder.addField(cdcDataSchema.getField(name));
      sourcePositions[i] = cdcDataSchema.indexOf(name);
    }
    Schema sourceBeamSchema = sourceBeamSchemaBuilder.build();

    return new PartitionShardPlan(
        sourceBeamSchema,
        sourcePositions,
        sourceIcebergSchema,
        new InternalRecordWrapper(sourceIcebergSchema.asStruct()),
        new PartitionKey(spec, sourceIcebergSchema),
        JavaHash.forType(spec.partitionType()));
  }

  /**
   * Computes the shard for {@code data}: the partition tuple's hash picks the block base, and
   * {@code offset} (in {@code [0, shardsPerPartition)}) selects the shard within the block.
   */
  int shardFor(Row data, int offset, int numShards) {
    List<@Nullable Object> values = new ArrayList<>(sourcePositions.length);
    for (int position : sourcePositions) {
      values.add(data.getValue(position));
    }
    Row sourceRow = Row.withSchema(sourceSchema).attachValues(values);
    partitionKey.partition(
        wrapper.wrap(IcebergUtils.beamRowToIcebergRecord(sourceIcebergSchema, sourceRow)));
    int base = TableSetup.shardForHash(partitionHash.hash(partitionKey), numShards);
    return Math.floorMod(base + offset, numShards);
  }
}
