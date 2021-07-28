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
import org.apache.beam.sdk.io.gcp.spanner.cdc.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionMetadata;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

// TODO: Add java docs
public class ChangeStreamRecordMapper {

  public List<ChangeStreamRecord> toChangeStreamRecords(
      String partitionToken,
      Struct row,
      ChangeStreamResultSetMetadata resultSetMetadata,
      PartitionRestrictionMetadata restrictionMetadata) {
    return row.getStructList(0).stream()
        .flatMap(
            struct ->
                toChangeStreamRecord(
                    partitionToken, struct, resultSetMetadata, restrictionMetadata))
        .collect(Collectors.toList());
  }

  // TODO: add validation of the internal structure / values of each record parsed
  private Stream<ChangeStreamRecord> toChangeStreamRecord(
      String partitionToken,
      Struct row,
      ChangeStreamResultSetMetadata resultSetMetadata,
      PartitionRestrictionMetadata restrictionMetadata) {

    final Stream<DataChangeRecord> dataChangeRecords =
        row.getStructList("data_change_record").stream()
            .filter(this::isNonNullDataChangeRecord)
            .map(
                struct ->
                    toDataChangeRecord(
                        partitionToken, struct, resultSetMetadata, restrictionMetadata));

    final Stream<HeartbeatRecord> heartbeatRecords =
        row.getStructList("heartbeat_record").stream()
            .filter(this::isNonNullHeartbeatRecord)
            .map(struct -> toHeartbeatRecord(struct, resultSetMetadata, restrictionMetadata));

    final Stream<ChildPartitionsRecord> childPartitionsRecords =
        row.getStructList("child_partitions_record").stream()
            .filter(this::isNonNullChildPartitionsRecord)
            .map(
                struct ->
                    toChildPartitionsRecord(
                        partitionToken, struct, resultSetMetadata, restrictionMetadata));

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
      String partitionToken,
      Struct row,
      ChangeStreamResultSetMetadata resultSetMetadata,
      PartitionRestrictionMetadata restrictionMetadata) {
    final Timestamp commitTimestamp = row.getTimestamp("commit_timestamp");
    return DataChangeRecord.newBuilder()
        .withPartitionToken(partitionToken)
        .withCommitTimestamp(commitTimestamp)
        // FIXME: The spec has this as server_transaction_id
        .withTransactionId(row.getString("transaction_id"))
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
        .withMetadata(
            changeStreamRecordMetadataFrom(commitTimestamp, restrictionMetadata, resultSetMetadata))
        .build();
  }

  private HeartbeatRecord toHeartbeatRecord(
      Struct row,
      ChangeStreamResultSetMetadata resultSetMetadata,
      PartitionRestrictionMetadata restrictionMetadata) {
    final Timestamp timestamp = row.getTimestamp("timestamp");

    return HeartbeatRecord.newBuilder()
        .withTimestamp(timestamp)
        .withMetadata(
            changeStreamRecordMetadataFrom(timestamp, restrictionMetadata, resultSetMetadata))
        .build();
  }

  private ChildPartitionsRecord toChildPartitionsRecord(
      String partitionToken,
      Struct row,
      ChangeStreamResultSetMetadata resultSetMetadata,
      PartitionRestrictionMetadata restrictionMetadata) {
    final Timestamp startTimestamp = row.getTimestamp("start_timestamp");

    return ChildPartitionsRecord.newBuilder()
        .withStartTimestamp(startTimestamp)
        .withRecordSequence(row.getString("record_sequence"))
        .withChildPartitions(
            row.getStructList("child_partitions").stream()
                .map(struct -> childPartitionFrom(partitionToken, struct))
                .collect(Collectors.toList()))
        .withMetadata(
            changeStreamRecordMetadataFrom(startTimestamp, restrictionMetadata, resultSetMetadata))
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
      Timestamp recordTimestamp,
      PartitionRestrictionMetadata restrictionMetadata,
      ChangeStreamResultSetMetadata resultSetMetadata) {
    return ChangeStreamRecordMetadata.newBuilder()
        .withRecordTimestamp(recordTimestamp)
        .withPartitionToken(restrictionMetadata.getPartitionToken())
        .withPartitionStartTimestamp(restrictionMetadata.getPartitionStartTimestamp())
        .withPartitionEndTimestamp(restrictionMetadata.getPartitionEndTimestamp())
        .withRestrictionInitializedAt(restrictionMetadata.getRestrictionInitializedAt())
        .withPartitionCreatedAt(restrictionMetadata.getPartitionCreatedAt())
        .withPartitionScheduledAt(restrictionMetadata.getPartitionScheduledAt())
        .withPartitionRunningAt(restrictionMetadata.getPartitionRunningAt())
        .withQueryStartedAt(resultSetMetadata.getQueryStartedAt())
        .withRecordStreamStartedAt(resultSetMetadata.getRecordStreamStartedAt())
        .withRecordStreamEndedAt(resultSetMetadata.getRecordStreamEndedAt())
        .withRecordReadAt(resultSetMetadata.getRecordReadAt())
        .withTotalStreamTimeMillis(resultSetMetadata.getTotalStreamDuration().getMillis())
        .withNumberOfRecordsRead(resultSetMetadata.getNumberOfRecordsRead())
        .build();
  }
}
