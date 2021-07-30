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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.util.TestStructMapper.recordsToStruct;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.cdc.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamResultSetMetadata;
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
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamRecordMapperTest {

  private ChangeStreamRecordMapper mapper;
  private ChangeStreamResultSetMetadata resultSetMetadata;
  private PartitionRestrictionMetadata restrictionMetadata;

  @Before
  public void setUp() {
    this.mapper = new ChangeStreamRecordMapper();
    this.resultSetMetadata =
        new ChangeStreamResultSetMetadata(
            Timestamp.ofTimeMicroseconds(1L),
            Timestamp.ofTimeMicroseconds(2L),
            Timestamp.ofTimeMicroseconds(3L),
            Timestamp.ofTimeMicroseconds(4L),
            Duration.millis(100),
            10_000L);
    this.restrictionMetadata =
        new PartitionRestrictionMetadata(
            "partitionToken",
            Timestamp.ofTimeMicroseconds(10L),
            Timestamp.ofTimeMicroseconds(11L),
            Timestamp.ofTimeMicroseconds(12L),
            Timestamp.ofTimeMicroseconds(13L),
            Timestamp.ofTimeMicroseconds(14L),
            Timestamp.ofTimeMicroseconds(15L));
  }

  @Test
  public void testMappingUpdateStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        DataChangeRecord.newBuilder()
            .withPartitionToken("partitionToken")
            .withCommitTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .withServerTransactionId("serverTransactionId")
            .withIsLastRecordInTransactionInPartition(true)
            .withRecordSequence("1")
            .withTableName("tableName")
            .withRowType(
                Arrays.asList(
                    new ColumnType("column1", new TypeCode("type1"), true, 1L),
                    new ColumnType("column2", new TypeCode("type2"), false, 2L)))
            .withMods(
                Collections.singletonList(
                    new Mod(
                        "{\"column1\": \"value1\"}",
                        "{\"column2\": \"oldValue2\"}",
                        "{\"column2\": \"newValue2\"}")))
            .withModType(ModType.UPDATE)
            .withValueCaptureType(ValueCaptureType.OLD_AND_NEW_VALUES)
            .withNumberOfRecordsInTransaction(10L)
            .withNumberOfPartitionsInTransaction(2L)
            .build();
    final Struct struct = recordsToStruct(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(
            "partitionToken", struct, resultSetMetadata, restrictionMetadata));
  }

  @Test
  public void testMappingInsertStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        DataChangeRecord.newBuilder()
            .withPartitionToken("partitionToken")
            .withCommitTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .withServerTransactionId("transactionId")
            .withIsLastRecordInTransactionInPartition(false)
            .withRecordSequence("1")
            .withTableName("tableName")
            .withRowType(
                Arrays.asList(
                    new ColumnType("column1", new TypeCode("type1"), true, 1L),
                    new ColumnType("column2", new TypeCode("type2"), false, 2L)))
            .withMods(
                Collections.singletonList(
                    new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")))
            .withModType(ModType.INSERT)
            .withValueCaptureType(ValueCaptureType.OLD_AND_NEW_VALUES)
            .withNumberOfRecordsInTransaction(10L)
            .withNumberOfPartitionsInTransaction(2L)
            .build();
    final Struct struct = recordsToStruct(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(
            "partitionToken", struct, resultSetMetadata, restrictionMetadata));
  }

  @Test
  public void testMappingDeleteStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        DataChangeRecord.newBuilder()
            .withPartitionToken("partitionToken")
            .withCommitTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .withServerTransactionId("transactionId")
            .withIsLastRecordInTransactionInPartition(false)
            .withRecordSequence("1")
            .withTableName("tableName")
            .withRowType(
                Arrays.asList(
                    new ColumnType("column1", new TypeCode("type1"), true, 1L),
                    new ColumnType("column2", new TypeCode("type2"), false, 2L)))
            .withMods(
                Collections.singletonList(
                    new Mod("{\"column1\": \"value1\"}", "{\"column2\": \"oldValue2\"}", null)))
            .withModType(ModType.DELETE)
            .withValueCaptureType(ValueCaptureType.OLD_AND_NEW_VALUES)
            .withNumberOfRecordsInTransaction(10L)
            .withNumberOfPartitionsInTransaction(2L)
            .build();
    final Struct struct = recordsToStruct(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(
            "partitionToken", struct, resultSetMetadata, restrictionMetadata));
  }

  @Test
  public void testMappingStructRowToHeartbeatRecord() {
    final HeartbeatRecord heartbeatRecord =
        HeartbeatRecord.newBuilder()
            .withTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .build();
    final Struct struct = recordsToStruct(heartbeatRecord);

    assertEquals(
        Collections.singletonList(heartbeatRecord),
        mapper.toChangeStreamRecords(
            "partitionToken", struct, resultSetMetadata, restrictionMetadata));
  }

  @Test
  public void testMappingStructRowToChildPartitionRecord() {
    final ChildPartitionsRecord childPartitionsRecord =
        ChildPartitionsRecord.newBuilder()
            .withStartTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .withRecordSequence("1")
            .withChildPartitions(
                Arrays.asList(
                    new ChildPartition(
                        "childToken1", Sets.newHashSet("parentToken1", "parentToken2")),
                    new ChildPartition(
                        "childToken2", Sets.newHashSet("parentToken1", "parentToken2"))))
            .build();
    final Struct struct = recordsToStruct(childPartitionsRecord);

    assertEquals(
        Collections.singletonList(childPartitionsRecord),
        mapper.toChangeStreamRecords(
            "partitionToken", struct, resultSetMetadata, restrictionMetadata));
  }

  /** Adds the default parent partition token as a parent of each child partition. */
  @Test
  public void testMappingStructRowFromInitialPartitionToChildPartitionRecord() {
    final Struct struct =
        recordsToStruct(
            ChildPartitionsRecord.newBuilder()
                .withStartTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
                .withRecordSequence("1")
                .withChildPartitions(
                    Arrays.asList(
                        new ChildPartition("childToken1", Sets.newHashSet()),
                        new ChildPartition("childToken2", Sets.newHashSet())))
                .build());
    final ChildPartitionsRecord expected =
        ChildPartitionsRecord.newBuilder()
            .withStartTimestamp(Timestamp.ofTimeSecondsAndNanos(10L, 20))
            .withRecordSequence("1")
            .withChildPartitions(
                Arrays.asList(
                    new ChildPartition(
                        "childToken1", Sets.newHashSet(InitialPartition.PARTITION_TOKEN)),
                    new ChildPartition(
                        "childToken2", Sets.newHashSet(InitialPartition.PARTITION_TOKEN))))
            .build();

    assertEquals(
        Collections.singletonList(expected),
        mapper.toChangeStreamRecords(
            InitialPartition.PARTITION_TOKEN, struct, resultSetMetadata, restrictionMetadata));
  }

  // TODO: Add test case for unknown record type
  // TODO: Add test case for malformed record
}
