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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJsonMapper {

  private static final Logger LOG = LoggerFactory.getLogger(TestJsonMapper.class);

  public static @Nullable String recordToJson(
      ChangeStreamRecord record, boolean useUnknownModType, boolean useUnknownValueCaptureType) {
    try {
      Value value;
      if (record instanceof DataChangeRecord) {
        value =
            recordValueFrom(
                (DataChangeRecord) record, useUnknownModType, useUnknownValueCaptureType);
      } else if (record instanceof ChildPartitionsRecord) {
        value = recordValueFrom((ChildPartitionsRecord) record);
      } else if (record instanceof HeartbeatRecord) {
        value = recordValueFrom((HeartbeatRecord) record);
      } else {
        throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
      }
      return JsonFormat.printer()
          .preservingProtoFieldNames()
          .omittingInsignificantWhitespace()
          .print(value);
    } catch (InvalidProtocolBufferException exc) {
      LOG.info("Failed to convert record to JSON: " + record.toString());
    }
    return null;
  }

  private static Value recordValueFrom(
      DataChangeRecord record, boolean useUnknownModType, boolean useUnknownValueCaptureType)
      throws InvalidProtocolBufferException {

    ListValue.Builder mods = ListValue.newBuilder();
    for (Mod mod : record.getMods()) {
      mods.addValues(modValueFrom(mod));
    }
    ListValue.Builder columnTypes = ListValue.newBuilder();
    for (ColumnType rowType : record.getRowType()) {
      columnTypes.addValues(columnTypeValueFrom(rowType));
    }
    final String modType = useUnknownModType ? "NEW_MOD_TYPE" : record.getModType().name();
    final String valueCaptureType =
        useUnknownValueCaptureType ? "NEW_VALUE_CAPTURE_TYPE" : record.getValueCaptureType().name();
    Value innerValue =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "commit_timestamp",
                        Value.newBuilder()
                            .setStringValue(record.getCommitTimestamp().toString())
                            .build())
                    .putFields(
                        "record_sequence",
                        Value.newBuilder().setStringValue(record.getRecordSequence()).build())
                    .putFields(
                        "server_transaction_id",
                        Value.newBuilder().setStringValue(record.getServerTransactionId()).build())
                    .putFields(
                        "is_last_record_in_transaction_in_partition",
                        Value.newBuilder()
                            .setBoolValue(record.isLastRecordInTransactionInPartition())
                            .build())
                    .putFields(
                        "table_name",
                        Value.newBuilder().setStringValue(record.getTableName()).build())
                    .putFields("mods", Value.newBuilder().setListValue(mods.build()).build())
                    .putFields(
                        "column_types",
                        Value.newBuilder().setListValue(columnTypes.build()).build())
                    .putFields("mod_type", Value.newBuilder().setStringValue(modType).build())
                    .putFields(
                        "value_capture_type",
                        Value.newBuilder().setStringValue(valueCaptureType).build())
                    .putFields(
                        "number_of_records_in_transaction",
                        Value.newBuilder()
                            .setNumberValue(record.getNumberOfRecordsInTransaction())
                            .build())
                    .putFields(
                        "number_of_partitions_in_transaction",
                        Value.newBuilder()
                            .setNumberValue(record.getNumberOfPartitionsInTransaction())
                            .build())
                    .putFields(
                        "transaction_tag",
                        Value.newBuilder().setStringValue(record.getTransactionTag()).build())
                    .putFields(
                        "is_system_transaction",
                        Value.newBuilder().setBoolValue(record.isSystemTransaction()).build()))
            .build();
    return Value.newBuilder()
        .setStructValue(Struct.newBuilder().putFields("data_change_record", innerValue).build())
        .build();
  }

  private static Value columnTypeValueFrom(ColumnType columnType)
      throws InvalidProtocolBufferException {
    Value.Builder typeValue = Value.newBuilder();
    String type = columnType.getType().getCode();
    try {
      JsonFormat.parser().ignoringUnknownFields().merge(type, typeValue);
    } catch (InvalidProtocolBufferException exc) {
      throw exc;
    }
    return Value.newBuilder()
        .setStructValue(
            Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue(columnType.getName()).build())
                .putFields("type", typeValue.build())
                .putFields(
                    "is_primary_key",
                    Value.newBuilder().setBoolValue(columnType.isPrimaryKey()).build())
                .putFields(
                    "ordinal_position",
                    Value.newBuilder().setNumberValue(columnType.getOrdinalPosition()).build())
                .build())
        .build();
  }

  private static Value modValueFrom(Mod mod) throws InvalidProtocolBufferException {
    Value.Builder keyValue = Value.newBuilder();
    try {
      JsonFormat.parser().ignoringUnknownFields().merge(mod.getKeysJson(), keyValue);
    } catch (InvalidProtocolBufferException exc) {
      throw exc;
    }
    Struct.Builder struct = Struct.newBuilder().putFields("keys", keyValue.build());
    if (mod.getOldValuesJson() != null) {
      Value.Builder oldValue = Value.newBuilder();
      try {
        JsonFormat.parser().ignoringUnknownFields().merge(mod.getOldValuesJson(), oldValue);
      } catch (InvalidProtocolBufferException exc) {
        throw exc;
      }
      struct.putFields("old_values", oldValue.build());
    }

    if (mod.getNewValuesJson() != null) {
      Value.Builder newValue = Value.newBuilder();
      try {
        JsonFormat.parser().ignoringUnknownFields().merge(mod.getNewValuesJson(), newValue);
      } catch (InvalidProtocolBufferException exc) {
        throw exc;
      }
      struct.putFields("new_values", newValue.build());
    }
    return Value.newBuilder().setStructValue(struct.build()).build();
  }

  private static Value recordValueFrom(HeartbeatRecord record) {
    Value innerValue =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "timestamp",
                        Value.newBuilder().setStringValue(record.getTimestamp().toString()).build())
                    .build())
            .build();
    return Value.newBuilder()
        .setStructValue(Struct.newBuilder().putFields("heartbeat_record", innerValue).build())
        .build();
  }

  private static Value recordValueFrom(ChildPartitionsRecord record) {
    ListValue.Builder listValue = ListValue.newBuilder();
    for (ChildPartition childPartition : record.getChildPartitions()) {
      listValue.addValues(childPartitionFrom(childPartition));
    }
    Value innerValue =
        Value.newBuilder()
            .setStructValue(
                Struct.newBuilder()
                    .putFields(
                        "start_timestamp",
                        Value.newBuilder()
                            .setStringValue(record.getStartTimestamp().toString())
                            .build())
                    .putFields(
                        "record_sequence",
                        Value.newBuilder().setStringValue(record.getRecordSequence()).build())
                    .putFields(
                        "child_partitions",
                        Value.newBuilder().setListValue(listValue.build()).build())
                    .build())
            .build();
    return Value.newBuilder()
        .setStructValue(
            Struct.newBuilder().putFields("child_partitions_record", innerValue).build())
        .build();
  }

  private static Value childPartitionFrom(ChildPartition childPartition) {
    ListValue.Builder listValue = ListValue.newBuilder();
    for (String parentToken : childPartition.getParentTokens()) {
      listValue.addValues(Value.newBuilder().setStringValue(parentToken).build());
    }
    return Value.newBuilder()
        .setStructValue(
            Struct.newBuilder()
                .putFields(
                    "token", Value.newBuilder().setStringValue(childPartition.getToken()).build())
                .putFields(
                    "parent_partition_tokens",
                    Value.newBuilder().setListValue(listValue.build()).build())
                .build())
        .build();
  }
}
