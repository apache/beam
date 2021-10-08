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
package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

// TODO: Add java docs
public class ChangeStreamRecordMapper {

  public List<ChangeStreamRecord> toChangeStreamRecords(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    return row.getStructList(0).stream()
        .flatMap(struct -> toChangeStreamRecord(partition, struct, resultSetMetadata))
        .collect(Collectors.toList());
  }

  // TODO: add validation of the internal structure / values of each record parsed
  private Stream<ChangeStreamRecord> toChangeStreamRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {

    final Stream<DataChangeRecord> dataChangeRecords =
        row.getStructList("data_change_record").stream()
            .filter(this::isNonNullDataChangeRecord)
            .map(struct -> toDataChangeRecord(partition, struct, resultSetMetadata));

    final Stream<HeartbeatRecord> heartbeatRecords =
        row.getStructList("heartbeat_record").stream()
            .filter(this::isNonNullHeartbeatRecord)
            .map(struct -> toHeartbeatRecord(partition, struct, resultSetMetadata));

    final Stream<ChildPartitionsRecord> childPartitionsRecords =
        row.getStructList("child_partitions_record").stream()
            .filter(this::isNonNullChildPartitionsRecord)
            .map(struct -> toChildPartitionsRecord(partition, struct, resultSetMetadata));

    return Stream.concat(
        Stream.concat(dataChangeRecords, heartbeatRecords), childPartitionsRecords);
  }

  private boolean isNonNullDataChangeRecord(Struct row) {
    return !row.isNull("commit_timestamp");
  }

  private boolean isNonNullHeartbeatRecord(Struct row) {
    return !row.isNull("timestamp");
  }

  private boolean isNonNullChildPartitionsRecord(Struct row) {
    return !row.isNull("start_timestamp");
  }

  private DataChangeRecord toDataChangeRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp commitTimestamp = row.getTimestamp("commit_timestamp");
    return DataChangeRecord.newBuilder()
        .withPartitionToken(partition.getPartitionToken())
        .withCommitTimestamp(commitTimestamp)
        .withServerTransactionId(row.getString("server_transaction_id"))
        .withIsLastRecordInTransactionInPartition(
            row.getBoolean("is_last_record_in_transaction_in_partition"))
        .withRecordSequence(row.getString("record_sequence"))
        .withTableName(row.getString("table_name"))
        .withRowType(
            row.getStructList("column_types").stream()
                .map(this::columnTypeFrom)
                .collect(Collectors.toList()))
        .withMods(
            row.getStructList("mods").stream().map(this::modFrom).collect(Collectors.toList()))
        .withModType(ModType.valueOf(row.getString("mod_type")))
        .withValueCaptureType(ValueCaptureType.valueOf(row.getString("value_capture_type")))
        .withNumberOfRecordsInTransaction(row.getLong("number_of_records_in_transaction"))
        .withNumberOfPartitionsInTransaction(row.getLong("number_of_partitions_in_transaction"))
        .withMetadata(changeStreamRecordMetadataFrom(partition, commitTimestamp, resultSetMetadata))
        .build();
  }

  private HeartbeatRecord toHeartbeatRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp timestamp = row.getTimestamp("timestamp");

    return HeartbeatRecord.newBuilder()
        .withTimestamp(timestamp)
        .withMetadata(changeStreamRecordMetadataFrom(partition, timestamp, resultSetMetadata))
        .build();
  }

  private ChildPartitionsRecord toChildPartitionsRecord(
      PartitionMetadata partition, Struct row, ChangeStreamResultSetMetadata resultSetMetadata) {
    final Timestamp startTimestamp = row.getTimestamp("start_timestamp");

    return ChildPartitionsRecord.newBuilder()
        .withStartTimestamp(startTimestamp)
        .withRecordSequence(row.getString("record_sequence"))
        .withChildPartitions(
            row.getStructList("child_partitions").stream()
                .map(struct -> childPartitionFrom(partition.getPartitionToken(), struct))
                .collect(Collectors.toList()))
        .withMetadata(changeStreamRecordMetadataFrom(partition, startTimestamp, resultSetMetadata))
        .build();
  }

  private ColumnType columnTypeFrom(Struct struct) {
    return new ColumnType(
        struct.getString("name"),
        new TypeCode(struct.getString("type")),
        struct.getBoolean("is_primary_key"),
        struct.getLong("ordinal_position"));
  }

  private Mod modFrom(Struct struct) {
    final String keysJson = struct.getString("keys");
    final String oldValuesJson =
        struct.isNull("old_values") ? null : struct.getString("old_values");
    final String newValuesJson =
        struct.isNull("new_values") ? null : struct.getString("new_values");
    return new Mod(keysJson, oldValuesJson, newValuesJson);
  }

  private ChildPartition childPartitionFrom(String partitionToken, Struct struct) {
    final HashSet<String> parentTokens =
        Sets.newHashSet(struct.getStringList("parent_partition_tokens"));
    if (InitialPartition.isInitialPartition(partitionToken)) {
      parentTokens.add(partitionToken);
    }
    return new ChildPartition(struct.getString("token"), parentTokens);
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
}
