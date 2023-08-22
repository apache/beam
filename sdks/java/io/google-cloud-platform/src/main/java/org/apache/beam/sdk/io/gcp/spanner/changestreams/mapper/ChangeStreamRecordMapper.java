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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/**
 * This class is responsible for transforming a {@link Struct} to a {@link List} of {@link
 * ChangeStreamRecord} models.
 *
 * <p>The change stream full specification can be seen in the internal documentation
 * https://docs.google.com/document/d/1nLlMGvQLIeUSDNmtoLT9vaQo0hVGl4CIf6iCSOkdIbA/edit#bookmark=id.fxgtygh8eony
 */
public class ChangeStreamRecordMapper {

  private static final String DATA_CHANGE_RECORD_COLUMN = "data_change_record";
  private static final String HEARTBEAT_RECORD_COLUMN = "heartbeat_record";
  private static final String CHILD_PARTITIONS_RECORD_COLUMN = "child_partitions_record";

  private static final String COMMIT_TIMESTAMP_COLUMN = "commit_timestamp";
  private static final String SERVER_TRANSACTION_ID_COLUMN = "server_transaction_id";
  private static final String IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN =
      "is_last_record_in_transaction_in_partition";
  private static final String RECORD_SEQUENCE_COLUMN = "record_sequence";
  private static final String TABLE_NAME_COLUMN = "table_name";
  private static final String COLUMN_TYPES_COLUMN = "column_types";
  private static final String MODS_COLUMN = "mods";
  private static final String MOD_TYPE_COLUMN = "mod_type";
  private static final String VALUE_CAPTURE_TYPE_COLUMN = "value_capture_type";
  private static final String NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN =
      "number_of_records_in_transaction";
  private static final String NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN =
      "number_of_partitions_in_transaction";
  private static final String TRANSACTION_TAG = "transaction_tag";
  private static final String IS_SYSTEM_TRANSACTION = "is_system_transaction";
  private static final String NAME_COLUMN = "name";
  private static final String TYPE_COLUMN = "type";
  private static final String IS_PRIMARY_KEY_COLUMN = "is_primary_key";
  private static final String ORDINAL_POSITION_COLUMN = "ordinal_position";
  private static final String KEYS_COLUMN = "keys";
  private static final String OLD_VALUES_COLUMN = "old_values";
  private static final String NEW_VALUES_COLUMN = "new_values";

  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final String START_TIMESTAMP_COLUMN = "start_timestamp";
  private static final String CHILD_PARTITIONS_COLUMN = "child_partitions";
  private static final String PARENT_PARTITION_TOKENS_COLUMN = "parent_partition_tokens";
  private static final String TOKEN_COLUMN = "token";
  private final Dialect dialect;
  private final JsonFormat.Printer printer;
  private final JsonFormat.Parser parser;

  ChangeStreamRecordMapper(Dialect dialect) {
    this.dialect = dialect;

    this.printer =
        JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();
    this.parser = JsonFormat.parser().ignoringUnknownFields();
  }

  /**
   * In GoogleSQL, change stream records are returned as an array of {@link Struct}. In PostgresQL,
   * change stream records are returned as {@link Jsonb} Transforms a {@link Struct / Jsonb}
   * representing a change stream result into a {@link List} of {@link ChangeStreamRecord} model.
   * The type of the change stream record will be identified and one of the following subclasses can
   * be returned within the resulting {@link List}:
   *
   * <ul>
   *   <li>{@link DataChangeRecord}
   *   <li>{@link HeartbeatRecord}
   *   <li>{@link ChildPartitionsRecord}
   * </ul>
   *
   * Additionally to the {@link Struct / Jsonb} received, the originating partition of the records
   * (given by the {@link PartitionMetadata} parameter) and the stream metadata (given by the {@link
   * ChangeStreamResultSetMetadata}) are used to populate the {@link ChangeStreamRecordMetadata} for
   * each record mapped.
   *
   * <p>The {@link Struct / Jsonb} is expected to have the following fields:
   *
   * <ul>
   *   <li>{@link ChangeStreamRecordMapper#DATA_CHANGE_RECORD_COLUMN}: non-nullable {@link Struct}
   *       list of data change records or a {@link Jsonb} representing a child partition record.
   *       <ul>
   *         <li>{@link ChangeStreamRecordMapper#COMMIT_TIMESTAMP_COLUMN}: non-nullable {@link
   *             Timestamp} representing the timestamp at which the modifications within the record
   *             were committed in Cloud Spanner.
   *         <li>{@link ChangeStreamRecordMapper#SERVER_TRANSACTION_ID_COLUMN}: non-nullable {@link
   *             String} representing the unique transaction id in which the modifications for this
   *             record occurred.
   *         <li>{@link ChangeStreamRecordMapper#IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN}:
   *             non-nullable {@link Boolean} indicating whether this record is the last emitted for
   *             the transaction.
   *         <li>{@link ChangeStreamRecordMapper#RECORD_SEQUENCE_COLUMN}: non-nullable {@link
   *             String} representing the order in which this record appears within the context of a
   *             partition, commit timestamp and transaction.
   *         <li>{@link ChangeStreamRecordMapper#TABLE_NAME_COLUMN}: non-nullable {@link String}
   *             representing the name of the table in which the modifications for this record
   *             occurred.
   *         <li>{@link ChangeStreamRecordMapper#COLUMN_TYPES_COLUMN}: non-nullable {@link List} of
   *             {@link Struct}s representing the type of the primary keys and modified columns
   *             within this record.
   *             <ul>
   *               <li>{@link ChangeStreamRecordMapper#NAME_COLUMN}: non-nullable {@link String}
   *                   representing the name of a column.
   *               <li>{@link ChangeStreamRecordMapper#TYPE_COLUMN}: non-nullable {@link String}
   *                   representing the type of a column.
   *               <li>{@link ChangeStreamRecordMapper#IS_PRIMARY_KEY_COLUMN}: non-nullable {@link
   *                   Boolean} indicating if the column is part of the primary key.
   *               <li>{@link ChangeStreamRecordMapper#ORDINAL_POSITION_COLUMN}: non-nullable {@link
   *                   Long} representing the position of the column in the table it is defined.
   *             </ul>
   *         <li>{@link ChangeStreamRecordMapper#MODS_COLUMN}: non-nullable {@link List} of {@link
   *             Struct}s representing the data modifications within this record.
   *             <ul>
   *               <li>{@link ChangeStreamRecordMapper#KEYS_COLUMN}: non-nullable {@link String}
   *                   json object, where keys are the primary key column names, and the values are
   *                   their corresponding values.
   *               <li>{@link ChangeStreamRecordMapper#OLD_VALUES_COLUMN}: nullable {@link String}
   *                   json object displaying the old state of the columns modified, where keys are
   *                   the column names, and the values are their corresponding values.
   *               <li>{@link ChangeStreamRecordMapper#NEW_VALUES_COLUMN}: nullable {@link String}
   *                   json object displaying the new state of the columns modified, where keys are
   *                   the column names, and the values are their corresponding values.
   *             </ul>
   *         <li>{@link ChangeStreamRecordMapper#MOD_TYPE_COLUMN}: non-nullable {@link String}
   *             representing the type of operation that caused the modifications (see also {@link
   *             ModType}.
   *         <li>{@link ChangeStreamRecordMapper#VALUE_CAPTURE_TYPE_COLUMN}: non-nullable {@link
   *             String} representing the capture type of the change stream that generated this
   *             record (see also {@link ValueCaptureType}).
   *         <li>{@link ChangeStreamRecordMapper#NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN}:
   *             non-nullable {@link Long} representing the total number of data change records for
   *             the transaction in which this record occurred.
   *         <li>{@link ChangeStreamRecordMapper#NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN}:
   *             non-nullable {@link Long} representing the total number of partitions for the
   *             transaction in which this record occurred.
   *       </ul>
   *   <li>{@link ChangeStreamRecordMapper#HEARTBEAT_RECORD_COLUMN}: non-nullable {@link Struct}
   *       list of hearbeat records or a {@link Jsonb} representing a child partition record.
   *       <ul>
   *         <li>{@link ChangeStreamRecordMapper#TIMESTAMP_COLUMN}: non-nullable {@link Timestamp}
   *             representing the timestamp for which the change stream query has returned all
   *             changes (see more in {@link HeartbeatRecord#getTimestamp()}.
   *       </ul>
   *   <li>{@link ChangeStreamRecordMapper#CHILD_PARTITIONS_RECORD_COLUMN}: non-nullable {@link
   *       Struct} list of child partitions records or a {@link Jsonb} representing a child
   *       partition record.
   *       <ul>
   *         <li>{@link ChangeStreamRecordMapper#START_TIMESTAMP_COLUMN}: non-nullable {@link
   *             Timestamp} representing the timestamp at which the new partition started being
   *             valid in Cloud Spanner.
   *         <li>{@link ChangeStreamRecordMapper#RECORD_SEQUENCE_COLUMN}: non-nullable {@link
   *             String} representing the order in which this record appears within the context of a
   *             partition and commit timestamp.
   *         <li>{@link ChangeStreamRecordMapper#CHILD_PARTITIONS_COLUMN}: non-nullable {@link List}
   *             of {@link Struct} representing the new child partitions.
   *             <ul>
   *               <li>{@link ChangeStreamRecordMapper#TOKEN_COLUMN}: non-nullable {@link String}
   *                   representing the unique identifier of the new child partition.
   *               <li>{@link ChangeStreamRecordMapper#PARENT_PARTITION_TOKENS_COLUMN}: non-nullable
   *                   {@link List} of {@link String} representing the unique identifier(s) of
   *                   parent partition(s) where this child partition originated from.
   *             </ul>
   *       </ul>
   * </ul>
   *
   * @param partition the partition metadata from which the row was generated
   * @param resultSet the change stream result set
   * @param resultSetMetadata the metadata generated when reading the change stream row
   * @return a {@link List} of {@link ChangeStreamRecord} subclasses
   */
  public List<ChangeStreamRecord> toChangeStreamRecords(
      PartitionMetadata partition,
      ChangeStreamResultSet resultSet,
      ChangeStreamResultSetMetadata resultSetMetadata) {
    if (this.isPostgres()) {
      // In PostgresQL, change stream records are returned as JsonB.
      return Collections.singletonList(
          toChangeStreamRecordJson(partition, resultSet.getPgJsonb(0), resultSetMetadata));
    }
    // In GoogleSQL, change stream records are returned as an array of structs.
    return resultSet.getCurrentRowAsStruct().getStructList(0).stream()
        .flatMap(struct -> toChangeStreamRecord(partition, struct, resultSetMetadata))
        .collect(Collectors.toList());
  }

  Stream<ChangeStreamRecord> toChangeStreamRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {

    final Stream<DataChangeRecord> dataChangeRecords =
        row.getStructList(DATA_CHANGE_RECORD_COLUMN).stream()
            .filter(this::isNonNullDataChangeRecord)
            .map(struct -> toDataChangeRecord(partition, struct, resultSetMetadata));

    final Stream<HeartbeatRecord> heartbeatRecords =
        row.getStructList(HEARTBEAT_RECORD_COLUMN).stream()
            .filter(this::isNonNullHeartbeatRecord)
            .map(struct -> toHeartbeatRecord(partition, struct, resultSetMetadata));

    final Stream<ChildPartitionsRecord> childPartitionsRecords =
        row.getStructList(CHILD_PARTITIONS_RECORD_COLUMN).stream()
            .filter(this::isNonNullChildPartitionsRecord)
            .map(struct -> toChildPartitionsRecord(partition, struct, resultSetMetadata));

    return Stream.concat(
        Stream.concat(dataChangeRecords, heartbeatRecords), childPartitionsRecords);
  }

  ChangeStreamRecord toChangeStreamRecordJson(
      PartitionMetadata partition, String row, ChangeStreamResultSetMetadata resultSetMetadata) {
    Value.Builder valueBuilder = Value.newBuilder();
    try {
      this.parser.merge(row, valueBuilder);
    } catch (InvalidProtocolBufferException exc) {
      throw new IllegalArgumentException("Failed to parse record into proto: " + row);
    }
    Value value = valueBuilder.build();
    if (isNonNullDataChangeRecordJson(value)) {
      return toDataChangeRecordJson(partition, value, resultSetMetadata);
    } else if (isNonNullHeartbeatRecordJson(value)) {
      return toHeartbeatRecordJson(partition, value, resultSetMetadata);
    } else if (isNonNullChildPartitionsRecordJson(value)) {
      return toChildPartitionsRecordJson(partition, value, resultSetMetadata);
    } else {
      throw new IllegalArgumentException("Unknown change stream record type " + row);
    }
  }

  private boolean isNonNullDataChangeRecord(Struct row) {
    return !row.isNull(COMMIT_TIMESTAMP_COLUMN);
  }

  private boolean isNonNullDataChangeRecordJson(Value row) {
    return row.getStructValue().getFieldsMap().containsKey(DATA_CHANGE_RECORD_COLUMN);
  }

  private boolean isNonNullHeartbeatRecord(Struct row) {
    return !row.isNull(TIMESTAMP_COLUMN);
  }

  private boolean isNonNullHeartbeatRecordJson(Value row) {
    return row.getStructValue().getFieldsMap().containsKey(HEARTBEAT_RECORD_COLUMN);
  }

  private boolean isNonNullChildPartitionsRecord(Struct row) {
    return !row.isNull(START_TIMESTAMP_COLUMN);
  }

  private boolean isNonNullChildPartitionsRecordJson(Value row) {
    return row.getStructValue().getFieldsMap().containsKey(CHILD_PARTITIONS_RECORD_COLUMN);
  }

  private DataChangeRecord toDataChangeRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp commitTimestamp = row.getTimestamp(COMMIT_TIMESTAMP_COLUMN);
    return new DataChangeRecord(
        partition.getPartitionToken(),
        commitTimestamp,
        row.getString(SERVER_TRANSACTION_ID_COLUMN),
        row.getBoolean(IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN),
        row.getString(RECORD_SEQUENCE_COLUMN),
        row.getString(TABLE_NAME_COLUMN),
        row.getStructList(COLUMN_TYPES_COLUMN).stream()
            .map(this::columnTypeFrom)
            .collect(Collectors.toList()),
        row.getStructList(MODS_COLUMN).stream().map(this::modFrom).collect(Collectors.toList()),
        modTypeFrom(row.getString(MOD_TYPE_COLUMN)),
        valueCaptureTypeFrom(row.getString(VALUE_CAPTURE_TYPE_COLUMN)),
        row.getLong(NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN),
        row.getLong(NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN),
        row.getString(TRANSACTION_TAG),
        row.getBoolean(IS_SYSTEM_TRANSACTION),
        changeStreamRecordMetadataFrom(partition, commitTimestamp, resultSetMetadata));
  }

  private DataChangeRecord toDataChangeRecordJson(
      PartitionMetadata partition, Value row, ChangeStreamResultSetMetadata resultSetMetadata) {
    Value dataChangeRecordValue =
        Optional.ofNullable(row.getStructValue().getFieldsMap().get(DATA_CHANGE_RECORD_COLUMN))
            .orElseThrow(IllegalArgumentException::new);
    Map<String, Value> valueMap = dataChangeRecordValue.getStructValue().getFieldsMap();
    final String commitTimestamp =
        Optional.ofNullable(valueMap.get(COMMIT_TIMESTAMP_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue();
    return new DataChangeRecord(
        partition.getPartitionToken(),
        Timestamp.parseTimestamp(commitTimestamp),
        Optional.ofNullable(valueMap.get(SERVER_TRANSACTION_ID_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        Optional.ofNullable(valueMap.get(IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getBoolValue(),
        Optional.ofNullable(valueMap.get(RECORD_SEQUENCE_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        Optional.ofNullable(valueMap.get(TABLE_NAME_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        Optional.ofNullable(valueMap.get(COLUMN_TYPES_COLUMN))
            .orElseThrow(IllegalArgumentException::new).getListValue().getValuesList().stream()
            .map(this::columnTypeJsonFrom)
            .collect(Collectors.toList()),
        Optional.ofNullable(valueMap.get(MODS_COLUMN)).orElseThrow(IllegalArgumentException::new)
            .getListValue().getValuesList().stream()
            .map(this::modJsonFrom)
            .collect(Collectors.toList()),
        modTypeFrom(
            Optional.ofNullable(valueMap.get(MOD_TYPE_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getStringValue()),
        valueCaptureTypeFrom(
            Optional.ofNullable(valueMap.get(VALUE_CAPTURE_TYPE_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getStringValue()),
        (long)
            Optional.ofNullable(valueMap.get(NUMBER_OF_RECORDS_IN_TRANSACTION_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getNumberValue(),
        (long)
            Optional.ofNullable(valueMap.get(NUMBER_OF_PARTITIONS_IN_TRANSACTION_COLUMN))
                .orElseThrow(IllegalArgumentException::new)
                .getNumberValue(),
        Optional.ofNullable(valueMap.get(TRANSACTION_TAG))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        Optional.ofNullable(valueMap.get(IS_SYSTEM_TRANSACTION))
            .orElseThrow(IllegalArgumentException::new)
            .getBoolValue(),
        changeStreamRecordMetadataFrom(
            partition, Timestamp.parseTimestamp(commitTimestamp), resultSetMetadata));
  }

  private HeartbeatRecord toHeartbeatRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp timestamp = row.getTimestamp(TIMESTAMP_COLUMN);

    return new HeartbeatRecord(
        timestamp, changeStreamRecordMetadataFrom(partition, timestamp, resultSetMetadata));
  }

  private HeartbeatRecord toHeartbeatRecordJson(
      PartitionMetadata partition, Value row, ChangeStreamResultSetMetadata resultSetMetadata) {
    Value heartBeatRecordValue =
        Optional.ofNullable(row.getStructValue().getFieldsMap().get(HEARTBEAT_RECORD_COLUMN))
            .orElseThrow(IllegalArgumentException::new);
    Map<String, Value> valueMap = heartBeatRecordValue.getStructValue().getFieldsMap();
    String heartbeatTimestamp =
        Optional.ofNullable(valueMap.get(TIMESTAMP_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue();

    return new HeartbeatRecord(
        Timestamp.parseTimestamp(heartbeatTimestamp),
        changeStreamRecordMetadataFrom(
            partition, Timestamp.parseTimestamp(heartbeatTimestamp), resultSetMetadata));
  }

  private ChildPartitionsRecord toChildPartitionsRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp startTimestamp = row.getTimestamp(START_TIMESTAMP_COLUMN);

    return new ChildPartitionsRecord(
        startTimestamp,
        row.getString(RECORD_SEQUENCE_COLUMN),
        row.getStructList(CHILD_PARTITIONS_COLUMN).stream()
            .map(struct -> childPartitionFrom(partition.getPartitionToken(), struct))
            .collect(Collectors.toList()),
        changeStreamRecordMetadataFrom(partition, startTimestamp, resultSetMetadata));
  }

  private ChildPartitionsRecord toChildPartitionsRecordJson(
      PartitionMetadata partition, Value row, ChangeStreamResultSetMetadata resultSetMetadata) {
    Value childPartitionsRecordValue =
        Optional.ofNullable(row.getStructValue().getFieldsMap().get(CHILD_PARTITIONS_RECORD_COLUMN))
            .orElseThrow(IllegalArgumentException::new);
    Map<String, Value> valueMap = childPartitionsRecordValue.getStructValue().getFieldsMap();
    String startTimestamp =
        Optional.ofNullable(valueMap.get(START_TIMESTAMP_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue();

    return new ChildPartitionsRecord(
        Timestamp.parseTimestamp(startTimestamp),
        Optional.ofNullable(valueMap.get(RECORD_SEQUENCE_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        Optional.ofNullable(valueMap.get(CHILD_PARTITIONS_COLUMN))
            .orElseThrow(IllegalArgumentException::new).getListValue().getValuesList().stream()
            .map(value -> childPartitionJsonFrom(partition.getPartitionToken(), value))
            .collect(Collectors.toList()),
        changeStreamRecordMetadataFrom(
            partition, Timestamp.parseTimestamp(startTimestamp), resultSetMetadata));
  }

  private ColumnType columnTypeFrom(Struct struct) {
    final String type = struct.getJson(TYPE_COLUMN);
    return new ColumnType(
        struct.getString(NAME_COLUMN),
        new TypeCode(type),
        struct.getBoolean(IS_PRIMARY_KEY_COLUMN),
        struct.getLong(ORDINAL_POSITION_COLUMN));
  }

  private ColumnType columnTypeJsonFrom(Value row) {
    Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
    try {
      final String type =
          this.printer.print(
              Optional.ofNullable(valueMap.get(TYPE_COLUMN))
                  .orElseThrow(IllegalArgumentException::new));
      return new ColumnType(
          Optional.ofNullable(valueMap.get(NAME_COLUMN))
              .orElseThrow(IllegalArgumentException::new)
              .getStringValue(),
          new TypeCode(type),
          Optional.ofNullable(valueMap.get(IS_PRIMARY_KEY_COLUMN))
              .orElseThrow(IllegalArgumentException::new)
              .getBoolValue(),
          (long)
              Optional.ofNullable(valueMap.get(ORDINAL_POSITION_COLUMN))
                  .orElseThrow(IllegalArgumentException::new)
                  .getNumberValue());
    } catch (InvalidProtocolBufferException exc) {
      throw new IllegalArgumentException("Failed to print type: " + row);
    }
  }

  private Mod modFrom(Struct struct) {
    final String keys = struct.getJson(KEYS_COLUMN);
    final String oldValues =
        struct.isNull(OLD_VALUES_COLUMN) ? null : struct.getJson(OLD_VALUES_COLUMN);
    final String newValues =
        struct.isNull(NEW_VALUES_COLUMN) ? null : struct.getJson(NEW_VALUES_COLUMN);
    return new Mod(keys, oldValues, newValues);
  }

  private Mod modJsonFrom(Value row) {
    try {
      Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
      final String keys =
          this.printer.print(
              Optional.ofNullable(valueMap.get(KEYS_COLUMN))
                  .orElseThrow(IllegalArgumentException::new));

      final String oldValues =
          !valueMap.containsKey("old_values")
              ? null
              : this.printer.print(
                  Optional.ofNullable(valueMap.get(OLD_VALUES_COLUMN))
                      .orElseThrow(IllegalArgumentException::new));
      final String newValues =
          !valueMap.containsKey("new_values")
              ? null
              : this.printer.print(
                  Optional.ofNullable(valueMap.get(NEW_VALUES_COLUMN))
                      .orElseThrow(IllegalArgumentException::new));
      return new Mod(keys, oldValues, newValues);
    } catch (InvalidProtocolBufferException exc) {
      throw new IllegalArgumentException("Failed to print mod: " + row);
    }
  }

  private ModType modTypeFrom(String name) {
    try {
      return ModType.valueOf(name);
    } catch (IllegalArgumentException e) {
      // This is not logged to prevent flooding users with messages
      return ModType.UNKNOWN;
    }
  }

  private ValueCaptureType valueCaptureTypeFrom(String name) {
    try {
      return ValueCaptureType.valueOf(name);
    } catch (IllegalArgumentException e) {
      // This is not logged to prevent flooding users with messages
      return ValueCaptureType.UNKNOWN;
    }
  }

  private ChildPartition childPartitionFrom(String partitionToken, Struct struct) {
    final HashSet<String> parentTokens =
        Sets.newHashSet(struct.getStringList(PARENT_PARTITION_TOKENS_COLUMN));
    if (InitialPartition.isInitialPartition(partitionToken)) {
      parentTokens.add(partitionToken);
    }
    return new ChildPartition(struct.getString(TOKEN_COLUMN), parentTokens);
  }

  private ChildPartition childPartitionJsonFrom(String partitionToken, Value row) {
    Map<String, Value> valueMap = row.getStructValue().getFieldsMap();
    final HashSet<String> parentTokens = Sets.newHashSet();
    for (Value parentToken :
        Optional.ofNullable(valueMap.get(PARENT_PARTITION_TOKENS_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getListValue()
            .getValuesList()) {
      parentTokens.add(parentToken.getStringValue());
    }
    if (InitialPartition.isInitialPartition(partitionToken)) {
      parentTokens.add(partitionToken);
    }
    return new ChildPartition(
        Optional.ofNullable(valueMap.get(TOKEN_COLUMN))
            .orElseThrow(IllegalArgumentException::new)
            .getStringValue(),
        parentTokens);
  }

  private ChangeStreamRecordMetadata changeStreamRecordMetadataFrom(
      PartitionMetadata partition,
      Timestamp recordTimestamp,
      ChangeStreamResultSetMetadata resultSetMetadata) {
    return ChangeStreamRecordMetadata.newBuilder()
        .withRecordTimestamp(recordTimestamp)
        .withPartitionToken(partition.getPartitionToken())
        .withPartitionStartTimestamp(partition.getStartTimestamp())
        .withPartitionEndTimestamp(partition.getEndTimestamp())
        .withPartitionCreatedAt(partition.getCreatedAt())
        .withPartitionScheduledAt(partition.getScheduledAt())
        .withPartitionRunningAt(partition.getRunningAt())
        .withQueryStartedAt(resultSetMetadata.getQueryStartedAt())
        .withRecordStreamStartedAt(resultSetMetadata.getRecordStreamStartedAt())
        .withRecordStreamEndedAt(resultSetMetadata.getRecordStreamEndedAt())
        .withRecordReadAt(resultSetMetadata.getRecordReadAt())
        .withTotalStreamTimeMillis(resultSetMetadata.getTotalStreamDuration().getMillis())
        .withNumberOfRecordsRead(resultSetMetadata.getNumberOfRecordsRead())
        .build();
  }

  private boolean isPostgres() {
    return this.dialect == Dialect.POSTGRESQL;
  }
}
