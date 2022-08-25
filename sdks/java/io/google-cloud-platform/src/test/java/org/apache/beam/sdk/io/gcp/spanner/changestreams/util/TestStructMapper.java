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
  // TODO: Remove COLUMN_TYPE_STRING_TYPE when backend has fully migrated to use JSON
  private static final Type COLUMN_TYPE_STRING_TYPE =
      Type.struct(
          StructField.of("name", Type.string()),
          StructField.of("type", Type.string()),
          StructField.of("is_primary_key", Type.bool()),
          StructField.of("ordinal_position", Type.int64()));
  private static final Type COLUMN_TYPE_JSON_TYPE =
      Type.struct(
          StructField.of("name", Type.string()),
          StructField.of("type", Type.json()),
          StructField.of("is_primary_key", Type.bool()),
          StructField.of("ordinal_position", Type.int64()));
  // TODO: Remove MOD_STRING_TYPE when backend has fully migrated to use JSON
  private static final Type MOD_STRING_TYPE =
      Type.struct(
          StructField.of("keys", Type.string()),
          StructField.of("new_values", Type.string()),
          StructField.of("old_values", Type.string()));
  private static final Type MOD_JSON_TYPE =
      Type.struct(
          StructField.of("keys", Type.json()),
          StructField.of("new_values", Type.json()),
          StructField.of("old_values", Type.json()));
  // TODO: Remove DATA_CHANGE_RECORD_STRING_TYPE when backend has fully migrated to use JSON
  private static final Type DATA_CHANGE_RECORD_STRING_TYPE =
      Type.struct(
          StructField.of("commit_timestamp", Type.timestamp()),
          StructField.of("record_sequence", Type.string()),
          StructField.of("server_transaction_id", Type.string()),
          StructField.of("is_last_record_in_transaction_in_partition", Type.bool()),
          StructField.of("table_name", Type.string()),
          StructField.of("column_types", Type.array(COLUMN_TYPE_STRING_TYPE)),
          StructField.of("mods", Type.array(MOD_STRING_TYPE)),
          StructField.of("mod_type", Type.string()),
          StructField.of("value_capture_type", Type.string()),
          StructField.of("number_of_records_in_transaction", Type.int64()),
          StructField.of("number_of_partitions_in_transaction", Type.int64()),
          StructField.of("transaction_tag", Type.string()),
          StructField.of("is_system_transaction", Type.bool()));
  private static final Type DATA_CHANGE_RECORD_JSON_TYPE =
      Type.struct(
          StructField.of("commit_timestamp", Type.timestamp()),
          StructField.of("record_sequence", Type.string()),
          StructField.of("server_transaction_id", Type.string()),
          StructField.of("is_last_record_in_transaction_in_partition", Type.bool()),
          StructField.of("table_name", Type.string()),
          StructField.of("column_types", Type.array(COLUMN_TYPE_JSON_TYPE)),
          StructField.of("mods", Type.array(MOD_JSON_TYPE)),
          StructField.of("mod_type", Type.string()),
          StructField.of("value_capture_type", Type.string()),
          StructField.of("number_of_records_in_transaction", Type.int64()),
          StructField.of("number_of_partitions_in_transaction", Type.int64()),
          StructField.of("transaction_tag", Type.string()),
          StructField.of("is_system_transaction", Type.bool()));
  private static final Type HEARTBEAT_RECORD_TYPE =
      Type.struct(StructField.of("timestamp", Type.timestamp()));
  private static final Type CHILD_PARTITIONS_RECORD_TYPE =
      Type.struct(
          StructField.of("start_timestamp", Type.timestamp()),
          StructField.of("record_sequence", Type.string()),
          StructField.of("child_partitions", Type.array(CHILD_PARTITION_TYPE)));
  // TODO: Remove STREAM_RECORD_STRING_TYPE when backend has fully migrated to use JSON
  private static final Type STREAM_RECORD_STRING_TYPE =
      Type.struct(
          StructField.of("data_change_record", Type.array(DATA_CHANGE_RECORD_STRING_TYPE)),
          StructField.of("heartbeat_record", Type.array(HEARTBEAT_RECORD_TYPE)),
          StructField.of("child_partitions_record", Type.array(CHILD_PARTITIONS_RECORD_TYPE)));
  private static final Type STREAM_RECORD_JSON_TYPE =
      Type.struct(
          StructField.of("data_change_record", Type.array(DATA_CHANGE_RECORD_JSON_TYPE)),
          StructField.of("heartbeat_record", Type.array(HEARTBEAT_RECORD_TYPE)),
          StructField.of("child_partitions_record", Type.array(CHILD_PARTITIONS_RECORD_TYPE)));

  public static Struct recordsToStructWithJson(ChangeStreamRecord... records) {
    return recordsToStruct(false, false, true, records);
  }

  public static Struct recordsWithUnknownModTypeAndValueCaptureType(ChangeStreamRecord... records) {
    return recordsToStruct(true, true, true, records);
  }

  // TODO: Remove when backend is fully migrated to JSON
  public static Struct recordsToStructWithStrings(ChangeStreamRecord... records) {
    return recordsToStruct(false, false, false, records);
  }

  private static Struct recordsToStruct(
      boolean useUnknownModType,
      boolean useUnknownValueCaptureType,
      boolean useJsonFields,
      ChangeStreamRecord... records) {
    final Type streamRecordType =
        useJsonFields ? STREAM_RECORD_JSON_TYPE : STREAM_RECORD_STRING_TYPE;
    return Struct.newBuilder()
        .add(
            Value.structArray(
                streamRecordType,
                Arrays.stream(records)
                    .map(
                        record ->
                            TestStructMapper.streamRecordStructFrom(
                                record,
                                useUnknownModType,
                                useUnknownValueCaptureType,
                                useJsonFields))
                    .collect(Collectors.toList())))
        .build();
  }

  private static Struct streamRecordStructFrom(
      ChangeStreamRecord record,
      boolean useUnknownModType,
      boolean useUnknownValueCaptureType,
      boolean useJsonFields) {
    if (record instanceof DataChangeRecord) {
      return streamRecordStructFrom(
          (DataChangeRecord) record, useUnknownModType, useUnknownValueCaptureType, useJsonFields);
    } else if (record instanceof HeartbeatRecord) {
      return streamRecordStructFrom((HeartbeatRecord) record, useJsonFields);
    } else if (record instanceof ChildPartitionsRecord) {
      return streamRecordStructFrom((ChildPartitionsRecord) record, useJsonFields);
    } else {
      throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
    }
  }

  private static Struct streamRecordStructFrom(
      ChildPartitionsRecord record, boolean useJsonFields) {
    final Type dataChangeRecordType =
        useJsonFields ? DATA_CHANGE_RECORD_JSON_TYPE : DATA_CHANGE_RECORD_STRING_TYPE;
    return Struct.newBuilder()
        .set("data_change_record")
        .to(Value.structArray(dataChangeRecordType, Collections.emptyList()))
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

  private static Struct streamRecordStructFrom(HeartbeatRecord record, boolean useJsonFields) {
    final Type dataChangeRecordType =
        useJsonFields ? DATA_CHANGE_RECORD_JSON_TYPE : DATA_CHANGE_RECORD_STRING_TYPE;
    return Struct.newBuilder()
        .set("data_change_record")
        .to(Value.structArray(dataChangeRecordType, Collections.emptyList()))
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

  private static Struct streamRecordStructFrom(
      DataChangeRecord record,
      boolean useUnknownModType,
      boolean useUnknownValueCaptureType,
      boolean useJsonFields) {
    final Type dataChangeRecordType =
        useJsonFields ? DATA_CHANGE_RECORD_JSON_TYPE : DATA_CHANGE_RECORD_STRING_TYPE;
    return Struct.newBuilder()
        .set("data_change_record")
        .to(
            Value.structArray(
                dataChangeRecordType,
                Collections.singletonList(
                    recordStructFrom(
                        record, useUnknownModType, useUnknownValueCaptureType, useJsonFields))))
        .set("heartbeat_record")
        .to(Value.structArray(HEARTBEAT_RECORD_TYPE, Collections.emptyList()))
        .set("child_partitions_record")
        .to(Value.structArray(CHILD_PARTITIONS_RECORD_TYPE, Collections.emptyList()))
        .build();
  }

  private static Struct recordStructFrom(
      DataChangeRecord record,
      boolean useUnknownModType,
      boolean useUnknownValueCaptureType,
      boolean useJsonFields) {
    final Value columnTypes =
        Value.structArray(
            useJsonFields ? COLUMN_TYPE_JSON_TYPE : COLUMN_TYPE_STRING_TYPE,
            record.getRowType().stream()
                .map(rowType -> TestStructMapper.columnTypeStructFrom(rowType, useJsonFields))
                .collect(Collectors.toList()));
    final Value mods =
        Value.structArray(
            useJsonFields ? MOD_JSON_TYPE : MOD_STRING_TYPE,
            record.getMods().stream()
                .map(mod -> TestStructMapper.modStructFrom(mod, useJsonFields))
                .collect(Collectors.toList()));
    final String modType = useUnknownModType ? "NEW_MOD_TYPE" : record.getModType().name();
    final String valueCaptureType =
        useUnknownValueCaptureType ? "NEW_VALUE_CAPTURE_TYPE" : record.getValueCaptureType().name();
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
        .to(modType)
        .set("value_capture_type")
        .to(valueCaptureType)
        .set("number_of_records_in_transaction")
        .to(record.getNumberOfRecordsInTransaction())
        .set("number_of_partitions_in_transaction")
        .to(record.getNumberOfPartitionsInTransaction())
        .set("transaction_tag")
        .to(record.getTransactionTag())
        .set("is_system_transaction")
        .to(record.isSystemTransaction())
        .build();
  }

  private static Struct columnTypeStructFrom(ColumnType columnType, boolean useJsonFields) {
    final Value type =
        useJsonFields
            ? Value.json(columnType.getType().getCode())
            : Value.string(columnType.getType().getCode());
    return Struct.newBuilder()
        .set("name")
        .to(columnType.getName())
        .set("type")
        .to(type)
        .set("is_primary_key")
        .to(columnType.isPrimaryKey())
        .set("ordinal_position")
        .to(columnType.getOrdinalPosition())
        .build();
  }

  private static Struct modStructFrom(Mod mod, boolean useJsonFields) {
    final Value keys =
        useJsonFields ? Value.json(mod.getKeysJson()) : Value.string(mod.getKeysJson());
    final Value newValues =
        useJsonFields ? Value.json(mod.getNewValuesJson()) : Value.string(mod.getNewValuesJson());
    final Value oldValues =
        useJsonFields ? Value.json(mod.getOldValuesJson()) : Value.string(mod.getOldValuesJson());
    return Struct.newBuilder()
        .set("keys")
        .to(keys)
        .set("new_values")
        .to(newValues)
        .set("old_values")
        .to(oldValues)
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
