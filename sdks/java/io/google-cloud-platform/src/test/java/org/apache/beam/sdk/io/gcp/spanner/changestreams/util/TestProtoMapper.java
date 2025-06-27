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
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionEndRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionEventRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionStartRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;

// Test util class to convert ChangeStreamRecord class to proto represenatation. Similar to
// TestJsonMapper and TestStructMapper.
public class TestProtoMapper {

  public static com.google.spanner.v1.ChangeStreamRecord recordToProto(ChangeStreamRecord record) {
    if (record instanceof PartitionStartRecord) {
      return convertPartitionStartRecordToProto((PartitionStartRecord) record);
    } else if (record instanceof PartitionEndRecord) {
      return convertPartitionEndRecordToProto((PartitionEndRecord) record);
    } else if (record instanceof PartitionEventRecord) {
      return convertPartitionEventRecordToProto((PartitionEventRecord) record);
    } else if (record instanceof HeartbeatRecord) {
      return convertHeartbeatRecordToProto((HeartbeatRecord) record);
    } else if (record instanceof DataChangeRecord) {
      return convertDataChangeRecordToProto((DataChangeRecord) record);
    } else {
      throw new UnsupportedOperationException("Unimplemented mapping for " + record.getClass());
    }
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionStartRecordToProto(
      PartitionStartRecord partitionStartRecord) {
    com.google.spanner.v1.ChangeStreamRecord.PartitionStartRecord partitionStartRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionStartRecord.newBuilder()
            .setStartTimestamp(partitionStartRecord.getStartTimestamp().toProto())
            .setRecordSequence(partitionStartRecord.getRecordSequence())
            .addAllPartitionTokens(partitionStartRecord.getPartitionTokens())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionStartRecord(partitionStartRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionEndRecordToProto(
      PartitionEndRecord partitionEndRecord) {
    com.google.spanner.v1.ChangeStreamRecord.PartitionEndRecord partitionEndRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionEndRecord.newBuilder()
            .setEndTimestamp(partitionEndRecord.getEndTimestamp().toProto())
            .setRecordSequence(partitionEndRecord.getRecordSequence())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionEndRecord(partitionEndRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertPartitionEventRecordToProto(
      PartitionEventRecord partitionEventRecord) {
    com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord partitionEventRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.PartitionEventRecord.newBuilder()
            .setCommitTimestamp(partitionEventRecord.getCommitTimestamp().toProto())
            .setRecordSequence(partitionEventRecord.getRecordSequence())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setPartitionEventRecord(partitionEventRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertHeartbeatRecordToProto(
      HeartbeatRecord heartbeatRecord) {
    com.google.spanner.v1.ChangeStreamRecord.HeartbeatRecord heartbeatRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.HeartbeatRecord.newBuilder()
            .setTimestamp(heartbeatRecord.getTimestamp().toProto())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setHeartbeatRecord(heartbeatRecordProto)
            .build();
    return changeStreamRecordProto;
  }

  private static com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType getProtoModType(
      ModType modType) {
    if (modType == ModType.INSERT) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType.INSERT;
    } else if (modType == ModType.UPDATE) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType.UPDATE;
    } else if (modType == ModType.DELETE) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType.DELETE;
    }
    return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModType.MOD_TYPE_UNSPECIFIED;
  }

  private static com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType
      getProtoValueCaptureTypeProto(ValueCaptureType valueCaptureType) {
    if (valueCaptureType == ValueCaptureType.NEW_ROW) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType.NEW_ROW;
    }
    if (valueCaptureType == ValueCaptureType.NEW_VALUES) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType.NEW_VALUES;
    }
    if (valueCaptureType == ValueCaptureType.OLD_AND_NEW_VALUES) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType
          .OLD_AND_NEW_VALUES;
    }
    if (valueCaptureType == ValueCaptureType.NEW_ROW_AND_OLD_VALUES) {
      return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType
          .NEW_ROW_AND_OLD_VALUES;
    }
    return com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ValueCaptureType
        .VALUE_CAPTURE_TYPE_UNSPECIFIED;
  }

  private static List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata>
      getProtoColumnMetadata(List<ColumnType> columnTypes) {
    JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();

    List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata>
        columnMetaDataProtos = new ArrayList<>();
    for (ColumnType columnType : columnTypes) {
      // TypeCode class contains json format type code, e.g. {\"code\":\"INT64\"}. We need to
      // extract "INT64" type code.
      Value.Builder typeCodeJson = Value.newBuilder();
      try {
        jsonParser.merge(columnType.getType().getCode(), typeCodeJson);
      } catch (InvalidProtocolBufferException exc) {
        throw new IllegalArgumentException(
            "Failed to parse json type code into proto: " + columnType.getType().getCode());
      }
      Value typeCode =
          Optional.ofNullable(typeCodeJson.build().getStructValue().getFieldsMap().get("code"))
              .orElseThrow(IllegalArgumentException::new);

      com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata columnMetadataProto =
          com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ColumnMetadata.newBuilder()
              .setName(columnType.getName())
              .setType(
                  com.google.spanner.v1.Type.newBuilder()
                      .setCode(com.google.spanner.v1.TypeCode.valueOf(typeCode.getStringValue())))
              .setIsPrimaryKey(columnType.isPrimaryKey())
              .setOrdinalPosition(columnType.getOrdinalPosition())
              .build();
      columnMetaDataProtos.add(columnMetadataProto);
    }
    return columnMetaDataProtos;
  }

  private static List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue>
      columnsJsonToProtos(String columnsJson, Map<String, Integer> columnNameToIndex) {
    List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue> modValueProtos =
        new ArrayList<>();
    JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();
    Value.Builder columnsJsonValue = Value.newBuilder();
    try {
      jsonParser.merge(columnsJson, columnsJsonValue);
    } catch (InvalidProtocolBufferException exc) {
      throw new IllegalArgumentException(
          "Failed to parse json type columns into proto: " + columnsJson);
    }
    Map<String, Value> columns = columnsJsonValue.build().getStructValue().getFieldsMap();
    for (Map.Entry<String, Value> entry : columns.entrySet()) {
      final String columnName = entry.getKey();
      final String columnValue = entry.getValue().getStringValue();
      final Integer columnIndex = columnNameToIndex.get(columnName);

      com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue modValueProto =
          com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.ModValue.newBuilder()
              .setColumnMetadataIndex(columnIndex)
              .setValue(Value.newBuilder().setStringValue(columnValue).build())
              .build();
      modValueProtos.add(modValueProto);
    }
    return modValueProtos;
  }

  private static List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.Mod> getProtoMods(
      List<Mod> mods, List<ColumnType> columnTypes) {
    Map<String, Integer> columnNameToIndex = new HashMap<>();
    for (int i = 0; i < columnTypes.size(); ++i) {
      columnNameToIndex.put(columnTypes.get(i).getName(), i);
    }
    List<com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.Mod> modProtos =
        new ArrayList<>();
    for (Mod mod : mods) {
      com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.Mod modProto =
          com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.Mod.newBuilder()
              .addAllKeys(columnsJsonToProtos(mod.getKeysJson(), columnNameToIndex))
              .addAllOldValues(columnsJsonToProtos(mod.getOldValuesJson(), columnNameToIndex))
              .addAllNewValues(columnsJsonToProtos(mod.getNewValuesJson(), columnNameToIndex))
              .build();
      modProtos.add(modProto);
    }
    return modProtos;
  }

  private static com.google.spanner.v1.ChangeStreamRecord convertDataChangeRecordToProto(
      DataChangeRecord dataChangeRecord) {
    com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord dataChangeRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.DataChangeRecord.newBuilder()
            .setCommitTimestamp(dataChangeRecord.getCommitTimestamp().toProto())
            .setRecordSequence(dataChangeRecord.getRecordSequence())
            .setServerTransactionId(dataChangeRecord.getServerTransactionId())
            .setIsLastRecordInTransactionInPartition(
                dataChangeRecord.isLastRecordInTransactionInPartition())
            .setTable(dataChangeRecord.getTableName())
            .addAllColumnMetadata(getProtoColumnMetadata(dataChangeRecord.getRowType()))
            .addAllMods(getProtoMods(dataChangeRecord.getMods(), dataChangeRecord.getRowType()))
            .setModType(getProtoModType(dataChangeRecord.getModType()))
            .setValueCaptureType(
                getProtoValueCaptureTypeProto(dataChangeRecord.getValueCaptureType()))
            .setNumberOfRecordsInTransaction(
                (int) dataChangeRecord.getNumberOfRecordsInTransaction())
            .setNumberOfPartitionsInTransaction(
                (int) dataChangeRecord.getNumberOfPartitionsInTransaction())
            .setTransactionTag(dataChangeRecord.getTransactionTag())
            .setIsSystemTransaction(dataChangeRecord.isSystemTransaction())
            .build();
    com.google.spanner.v1.ChangeStreamRecord changeStreamRecordProto =
        com.google.spanner.v1.ChangeStreamRecord.newBuilder()
            .setDataChangeRecord(dataChangeRecordProto)
            .build();
    return changeStreamRecordProto;
  }
}
