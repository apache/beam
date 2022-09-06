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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsToStructWithJson;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsToStructWithStrings;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsWithUnknownModTypeAndValueCaptureType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSetMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamRecordMapperTest {

  private ChangeStreamRecordMapper mapper;
  private PartitionMetadata partition;
  private ChangeStreamResultSetMetadata resultSetMetadata;

  @Before
  public void setUp() {
    mapper = new ChangeStreamRecordMapper();
    partition =
        PartitionMetadata.newBuilder()
            .setPartitionToken("partitionToken")
            .setParentTokens(Sets.newHashSet("parentToken"))
            .setHeartbeatMillis(30_000L)
            .setState(State.RUNNING)
            .setWatermark(Timestamp.ofTimeMicroseconds(10L))
            .setStartTimestamp(Timestamp.ofTimeMicroseconds(11L))
            .setEndTimestamp(Timestamp.ofTimeMicroseconds(12L))
            .setCreatedAt(Timestamp.ofTimeMicroseconds(13L))
            .setScheduledAt(Timestamp.ofTimeMicroseconds(14L))
            .setRunningAt(Timestamp.ofTimeMicroseconds(15L))
            .build();
    resultSetMetadata = mock(ChangeStreamResultSetMetadata.class);
    when(resultSetMetadata.getQueryStartedAt()).thenReturn(Timestamp.ofTimeMicroseconds(1L));
    when(resultSetMetadata.getRecordStreamStartedAt()).thenReturn(Timestamp.ofTimeMicroseconds(2L));
    when(resultSetMetadata.getRecordStreamEndedAt()).thenReturn(Timestamp.ofTimeMicroseconds(3L));
    when(resultSetMetadata.getRecordReadAt()).thenReturn(Timestamp.ofTimeMicroseconds(4L));
    when(resultSetMetadata.getTotalStreamDuration()).thenReturn(Duration.millis(100));
    when(resultSetMetadata.getNumberOfRecordsRead()).thenReturn(10_000L);
  }

  @Test
  public void testMappingUpdateStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod(
                    "{\"column1\": \"value1\"}",
                    "{\"column2\": \"oldValue2\"}",
                    "{\"column2\": \"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  /*
   * Change streams with NEW_ROW value capture type do not track old values, so null value
   * is used for OLD_VALUES_COLUMN in Mod.
   */
  @Test
  public void testMappingUpdateStructRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  /*
   * Change streams with NEW_VALUES value capture type do not track old values, so null value
   * is used for OLD_VALUES_COLUMN in Mod.
   */
  @Test
  public void testMappingUpdateStructRowNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingInsertStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingInsertStructRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingInsertStructRowNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteStructRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", "{\"column2\": \"oldValue2\"}", null)),
            ModType.DELETE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteStructRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\": \"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteStructRowNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\": \"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct stringFieldsStruct = recordsToStructWithStrings(dataChangeRecord);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, stringFieldsStruct, resultSetMetadata));
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, jsonFieldsStruct, resultSetMetadata));
  }

  @Test
  public void testMappingStructRowWithUnknownModTypeAndValueCaptureTypeToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\": \"value1\"}", null, "{\"column2\": \"newValue2\"}")),
            ModType.UNKNOWN,
            ValueCaptureType.UNKNOWN,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct struct = recordsWithUnknownModTypeAndValueCaptureType(dataChangeRecord);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, struct, resultSetMetadata));
  }

  @Test
  public void testMappingStructRowToHeartbeatRecord() {
    final HeartbeatRecord heartbeatRecord =
        new HeartbeatRecord(Timestamp.ofTimeSecondsAndNanos(10L, 20), null);
    final Struct struct = recordsToStructWithStrings(heartbeatRecord);

    assertEquals(
        Collections.singletonList(heartbeatRecord),
        mapper.toChangeStreamRecords(partition, struct, resultSetMetadata));
  }

  @Test
  public void testMappingStructRowToChildPartitionRecord() {
    final ChildPartitionsRecord childPartitionsRecord =
        new ChildPartitionsRecord(
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "1",
            Arrays.asList(
                new ChildPartition("childToken1", Sets.newHashSet("parentToken1", "parentToken2")),
                new ChildPartition("childToken2", Sets.newHashSet("parentToken1", "parentToken2"))),
            null);
    final Struct struct = recordsToStructWithStrings(childPartitionsRecord);

    assertEquals(
        Collections.singletonList(childPartitionsRecord),
        mapper.toChangeStreamRecords(partition, struct, resultSetMetadata));
  }

  /** Adds the default parent partition token as a parent of each child partition. */
  @Test
  public void testMappingStructRowFromInitialPartitionToChildPartitionRecord() {
    final Struct struct =
        recordsToStructWithStrings(
            new ChildPartitionsRecord(
                Timestamp.ofTimeSecondsAndNanos(10L, 20),
                "1",
                Arrays.asList(
                    new ChildPartition("childToken1", Sets.newHashSet()),
                    new ChildPartition("childToken2", Sets.newHashSet())),
                null));
    final ChildPartitionsRecord expected =
        new ChildPartitionsRecord(
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "1",
            Arrays.asList(
                new ChildPartition(
                    "childToken1", Sets.newHashSet(InitialPartition.PARTITION_TOKEN)),
                new ChildPartition(
                    "childToken2", Sets.newHashSet(InitialPartition.PARTITION_TOKEN))),
            null);

    final PartitionMetadata initialPartition =
        partition.toBuilder().setPartitionToken(InitialPartition.PARTITION_TOKEN).build();

    assertEquals(
        Collections.singletonList(expected),
        mapper.toChangeStreamRecords(initialPartition, struct, resultSetMetadata));
  }
}
