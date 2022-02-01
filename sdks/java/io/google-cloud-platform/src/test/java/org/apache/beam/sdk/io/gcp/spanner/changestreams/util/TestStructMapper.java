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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.util;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;

public class TestStructMapper {

  private static final Type CHILD_PARTITION_TYPE =
      Type.struct(
          StructField.of("token", Type.string()),
          StructField.of("parent_partition_tokens", Type.array(Type.string())));
  private static final Type COLUMN_TYPE_TYPE =
      Type.struct(
          StructField.of("name", Type.string()),
          StructField.of("type", Type.string()),
          StructField.of("is_primary_key", Type.bool()),
          StructField.of("ordinal_position", Type.int64()));
  private static final Type MOD_TYPE =
      Type.struct(
          StructField.of("keys", Type.string()),
          StructField.of("new_values", Type.string()),
          StructField.of("old_values", Type.string()));
  private static final Type DATA_CHANGE_RECORD_TYPE =
      Type.struct(
          StructField.of("commit_timestamp", Type.timestamp()),
          StructField.of("record_sequence", Type.string()),
          StructField.of("server_transaction_id", Type.string()),
          StructField.of("is_last_record_in_transaction_in_partition", Type.bool()),
          StructField.of("table_name", Type.string()),
          StructField.of("column_types", Type.array(COLUMN_TYPE_TYPE)),
          StructField.of("mods", Type.array(MOD_TYPE)),
          StructField.of("mod_type", Type.string()),
          StructField.of("value_capture_type", Type.string()),
          StructField.of("number_of_records_in_transaction", Type.int64()),
          StructField.of("number_of_partitions_in_transaction", Type.int64()));
  private static final Type HEARTBEAT_RECORD_TYPE =
      Type.struct(StructField.of("timestamp", Type.timestamp()));
  private static final Type CHILD_PARTITIONS_RECORD_TYPE =
      Type.struct(
          StructField.of("start_timestamp", Type.timestamp()),
          StructField.of("record_sequence", Type.string()),
          StructField.of("child_partitions", Type.array(CHILD_PARTITION_TYPE)));
  private static final Type STREAM_RECORD_TYPE =
      Type.struct(
          StructField.of("data_change_record", Type.array(DATA_CHANGE_RECORD_TYPE)),
          StructField.of("heartbeat_record", Type.array(HEARTBEAT_RECORD_TYPE)),
          StructField.of("child_partitions_record", Type.array(CHILD_PARTITIONS_RECORD_TYPE)));

  public static Struct recordsToStruct(ChangeStreamRecord... records) {
    return Struct.newBuilder()
        .add(
            Value.structArray(
                STREAM_RECORD_TYPE,
                Arrays.stream(records)
                    .map(TestStructMapper::streamRecordStructFrom)
                    .collect(Collectors.toList())))
        .build();
  }

  private static Struct streamRecordStructFrom(ChangeStreamRecord record) {
    if (record instanceof DataChangeRecord) {
      return streamRecordStructFrom((DataChangeRecord) record);
    } else if (record instanceof HeartbeatRecord) {
      return streamRecordStructFrom((HeartbeatRecord) record);
    } else if (record instanceof ChildPartitionsRecord) {
      return streamRecordStructFrom((ChildPartitionsRecord) record);
    } else {
      throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
    }
  }

  private static Struct streamRecordStructFrom(ChildPartitionsRecord record) {
    return Struct.newBuilder()
        .set("data_change_record")
        .to(Value.structArray(DATA_CHANGE_RECORD_TYPE, Collections.emptyList()))
        .set("heartbeat_record")
        .to(Value.structArray(HEARTBEAT_RECORD_TYPE, Collections.emptyList()))
        .set("child_partitions_record")
        .to(
            Value.structArray(
                CHILD_PARTITIONS_RECORD_TYPE, Collections.singletonList(recordStructFrom(record))))
        .build();
  }

  private static Struct recordStructFrom(ChildPartitionsRecord record) {
    final Value childPartitions =
        Value.structArray(
            CHILD_PARTITION_TYPE,
            record.getChildPartitions().stream()
                .map(TestStructMapper::childPartitionFrom)
                .collect(Collectors.toList()));
    return Struct.newBuilder()
        .set("start_timestamp")
        .to(record.getStartTimestamp())
        .set("record_sequence")
        .to(record.getRecordSequence())
        .set("child_partitions")
        .to(childPartitions)
        .build();
  }

  private static Struct streamRecordStructFrom(HeartbeatRecord record) {
    return Struct.newBuilder()
        .set("data_change_record")
        .to(Value.structArray(DATA_CHANGE_RECORD_TYPE, Collections.emptyList()))
        .set("heartbeat_record")
        .to(
            Value.structArray(
                HEARTBEAT_RECORD_TYPE, Collections.singletonList(recordStructFrom(record))))
        .set("child_partitions_record")
        .to(Value.structArray(CHILD_PARTITIONS_RECORD_TYPE, Collections.emptyList()))
        .build();
  }

  private static Struct recordStructFrom(HeartbeatRecord record) {
    return Struct.newBuilder().set("timestamp").to(record.getTimestamp()).build();
  }

  private static Struct streamRecordStructFrom(DataChangeRecord record) {
    return Struct.newBuilder()
        .set("data_change_record")
        .to(
            Value.structArray(
                DATA_CHANGE_RECORD_TYPE, Collections.singletonList(recordStructFrom(record))))
        .set("heartbeat_record")
        .to(Value.structArray(HEARTBEAT_RECORD_TYPE, Collections.emptyList()))
        .set("child_partitions_record")
        .to(Value.structArray(CHILD_PARTITIONS_RECORD_TYPE, Collections.emptyList()))
        .build();
  }

  private static Struct recordStructFrom(DataChangeRecord record) {
    final Value columnTypes =
        Value.structArray(
            COLUMN_TYPE_TYPE,
            record.getRowType().stream()
                .map(TestStructMapper::columnTypeStructFrom)
                .collect(Collectors.toList()));
    final Value mods =
        Value.structArray(
            MOD_TYPE,
            record.getMods().stream()
                .map(TestStructMapper::modStructFrom)
                .collect(Collectors.toList()));
    return Struct.newBuilder()
        .set("commit_timestamp")
        .to(record.getCommitTimestamp())
        .set("record_sequence")
        .to(record.getRecordSequence())
        .set("server_transaction_id")
        .to(record.getServerTransactionId())
        .set("is_last_record_in_transaction_in_partition")
        .to(record.isLastRecordInTransactionInPartition())
        .set("table_name")
        .to(record.getTableName())
        .set("column_types")
        .to(columnTypes)
        .set("mods")
        .to(mods)
        .set("mod_type")
        .to(record.getModType().toString())
        .set("value_capture_type")
        .to(record.getValueCaptureType().toString())
        .set("number_of_records_in_transaction")
        .to(record.getNumberOfRecordsInTransaction())
        .set("number_of_partitions_in_transaction")
        .to(record.getNumberOfPartitionsInTransaction())
        .build();
  }

  private static Struct columnTypeStructFrom(ColumnType columnType) {
    return Struct.newBuilder()
        .set("name")
        .to(columnType.getName())
        .set("type")
        .to(columnType.getType().getCode())
        .set("is_primary_key")
        .to(columnType.isPrimaryKey())
        .set("ordinal_position")
        .to(columnType.getOrdinalPosition())
        .build();
  }

  private static Struct modStructFrom(Mod mod) {
    return Struct.newBuilder()
        .set("keys")
        .to(mod.getKeysJson())
        .set("new_values")
        .to(mod.getNewValuesJson())
        .set("old_values")
        .to(mod.getOldValuesJson())
        .build();
  }

  private static Struct childPartitionFrom(ChildPartition childPartition) {
    return Struct.newBuilder()
        .set("token")
        .to(childPartition.getToken())
        .set("parent_partition_tokens")
        .to(Value.stringArray(childPartition.getParentTokens()))
        .build();
  }
}
