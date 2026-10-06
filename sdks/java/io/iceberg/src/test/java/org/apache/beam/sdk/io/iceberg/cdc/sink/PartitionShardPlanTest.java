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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.types.JavaHash;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PartitionShardPlan}. */
@RunWith(JUnit4.class)
public class PartitionShardPlanTest {

  private static final int NUM_SHARDS = 8;

  private static final org.apache.iceberg.Schema TABLE_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "region", Types.StringType.get()),
          Types.NestedField.optional(3, "name", Types.StringType.get()));

  private static final Schema DATA_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addStringField("region")
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

  private static final PartitionSpec BY_REGION =
      PartitionSpec.builderFor(TABLE_SCHEMA).identity("region").build();

  private static Row row(int id, String region, String name) {
    return Row.withSchema(DATA_SCHEMA).addValues(id, region, name).build();
  }

  @Test
  public void offsetSelectsAShardWithinThePartitionsBlock() {
    PartitionShardPlan plan = PartitionShardPlan.of(BY_REGION, TABLE_SCHEMA, DATA_SCHEMA);
    Row row = row(1, "us", "a");
    int base = plan.shardFor(row, 0, NUM_SHARDS);

    for (int offset = 0; offset < NUM_SHARDS; offset++) {
      int shard = plan.shardFor(row, offset, NUM_SHARDS);
      assertThat(shard, equalTo(Math.floorMod(base + offset, NUM_SHARDS)));
      assertThat(shard, greaterThanOrEqualTo(0));
      assertThat(shard, lessThan(NUM_SHARDS));
    }
  }

  @Test
  public void samePartitionTupleSharesABaseRegardlessOfOtherColumns() {
    PartitionShardPlan plan = PartitionShardPlan.of(BY_REGION, TABLE_SCHEMA, DATA_SCHEMA);

    assertThat(
        plan.shardFor(row(1, "us", "a"), 0, NUM_SHARDS),
        equalTo(plan.shardFor(row(2, "us", null), 0, NUM_SHARDS)));
    assertThat(
        plan.shardFor(row(1, "us", "a"), 3, NUM_SHARDS),
        equalTo(plan.shardFor(row(1, "us", "a"), 3, NUM_SHARDS)));
  }

  // The block base is the sink's shard of Iceberg's own hash of the partition tuple, so a plan
  // built from the Beam row agrees with a PartitionKey filled from an Iceberg record.
  @Test
  public void hashesTheIcebergPartitionTuple() {
    PartitionShardPlan plan = PartitionShardPlan.of(BY_REGION, TABLE_SCHEMA, DATA_SCHEMA);

    GenericRecord record = GenericRecord.create(TABLE_SCHEMA);
    record.setField("id", 1);
    record.setField("region", "eu");
    record.setField("name", "b");
    PartitionKey key = new PartitionKey(BY_REGION, TABLE_SCHEMA);
    key.partition(new InternalRecordWrapper(TABLE_SCHEMA.asStruct()).wrap(record));
    JavaHash<StructLike> hash = JavaHash.forType(BY_REGION.partitionType());
    int expected = TableSetup.shardForHash(hash.hash(key), NUM_SHARDS);

    assertThat(plan.shardFor(row(1, "eu", "b"), 0, NUM_SHARDS), equalTo(expected));
  }

  // One source column feeding two partition fields is converted once.
  @Test
  public void oneSourceColumnCanFeedSeveralPartitionFields() {
    PartitionSpec spec =
        PartitionSpec.builderFor(TABLE_SCHEMA).identity("region").truncate("region", 1).build();
    PartitionShardPlan plan = PartitionShardPlan.of(spec, TABLE_SCHEMA, DATA_SCHEMA);

    int shard = plan.shardFor(row(1, "us", "a"), 0, NUM_SHARDS);
    assertThat(shard, equalTo(plan.shardFor(row(9, "us", "z"), 0, NUM_SHARDS)));
    assertThat(shard, greaterThanOrEqualTo(0));
    assertThat(shard, lessThan(NUM_SHARDS));
  }
}
