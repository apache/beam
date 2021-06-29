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

import com.google.cloud.spanner.Struct;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.cdc.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;

// TODO: Add java docs
public class ChangeStreamRecordMapper {

  private final Gson gson;

  public ChangeStreamRecordMapper(Gson gson) {
    this.gson = gson;
  }

  public List<ChangeStreamRecord> toChangeStreamRecords(String partitionToken, Struct row) {
    return row.getStructList(0).stream()
        .flatMap(struct -> toChangeStreamRecord(partitionToken, struct))
        .collect(Collectors.toList());
  }

  // TODO: add validation of the internal structure / values of each record parsed
  private Stream<ChangeStreamRecord> toChangeStreamRecord(String partitionToken, Struct row) {
    final Stream<DataChangeRecord> dataChangeRecords =
        row.getStructList("data_change_record").stream()
            .filter(this::isNonNullDataChangeRecord)
            .map(struct -> toDataChangeRecord(partitionToken, struct));
    final Stream<HeartbeatRecord> heartbeatRecords =
        row.getStructList("heartbeat_record").stream()
            .filter(this::isNonNullHeartbeatRecord)
            .map(this::toHeartbeatRecord);
    final Stream<ChildPartitionsRecord> childPartitionsRecords =
        row.getStructList("child_partitions_record").stream()
            .filter(this::isNonNullChildPartitionsRecord)
            .map(struct -> toChildPartitionsRecord(partitionToken, struct));
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

  private DataChangeRecord toDataChangeRecord(String partitionToken, Struct row) {
    return new DataChangeRecord(
        partitionToken,
        row.getTimestamp("commit_timestamp"),
        // FIXME: The spec has this as server_transaction_id
        row.getString("transaction_id"),
        row.getBoolean("is_last_record_in_transaction_in_partition"),
        // FIXME: The spec has this as a String, but an int64 is returned
        row.getLong("record_sequence") + "",
        row.getString("table_name"),
        row.getStructList("column_types").stream()
            .map(this::columnTypeFrom)
            .collect(Collectors.toList()),
        row.getStructList("mods").stream().map(this::modFrom).collect(Collectors.toList()),
        ModType.valueOf(row.getString("mod_type")),
        ValueCaptureType.valueOf(row.getString("value_capture_type")),
        row.getLong("number_of_records_in_transaction"),
        row.getLong("number_of_partitions_in_transaction"));
  }

  private HeartbeatRecord toHeartbeatRecord(Struct row) {
    return new HeartbeatRecord(row.getTimestamp("timestamp"));
  }

  private ChildPartitionsRecord toChildPartitionsRecord(String partitionToken, Struct row) {
    return new ChildPartitionsRecord(
        row.getTimestamp("start_timestamp"),
        // FIXME: The spec has this as a String, but an int64 is returned
        row.getLong("record_sequence") + "",
        row.getStructList("child_partitions").stream()
            .map(struct -> childPartitionFrom(partitionToken, struct))
            .collect(Collectors.toList()));
  }

  private ColumnType columnTypeFrom(Struct struct) {
    return new ColumnType(
        struct.getString("name"),
        new TypeCode(struct.getString("type")),
        struct.getBoolean("is_primary_key"),
        struct.getLong("ordinal_position"));
  }

  private Mod modFrom(Struct struct) {
    final Map<String, String> keys = gson.fromJson(struct.getString("keys"), Map.class);
    final Map<String, String> oldValues = gson.fromJson(struct.getString("old_values"), Map.class);
    final Map<String, String> newValues = gson.fromJson(struct.getString("new_values"), Map.class);
    return new Mod(keys, oldValues, newValues);
  }

  private ChildPartition childPartitionFrom(String partitionToken, Struct struct) {
    final HashSet<String> parentTokens =
        Sets.newHashSet(struct.getStringList("parent_partition_tokens"));
    if (InitialPartition.isInitialPartition(partitionToken)) {
      parentTokens.add(partitionToken);
    }
    return new ChildPartition(struct.getString("token"), parentTokens);
  }
}
