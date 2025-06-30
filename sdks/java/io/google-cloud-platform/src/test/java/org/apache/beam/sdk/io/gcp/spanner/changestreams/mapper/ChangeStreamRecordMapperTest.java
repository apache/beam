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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestJsonMapper.recordToJson;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsToStructWithJson;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsToStructWithStrings;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestStructMapper.recordsWithUnknownModTypeAndValueCaptureType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamRecordMapperTest {

  private ChangeStreamRecordMapper mapper;
  private ChangeStreamRecordMapper mapperPostgres;
  private PartitionMetadata partition;
  private ChangeStreamResultSetMetadata resultSetMetadata;

  @Before
  public void setUp() {
    mapper = new ChangeStreamRecordMapper(Dialect.GOOGLE_STANDARD_SQL);
    mapperPostgres = new ChangeStreamRecordMapper(Dialect.POSTGRESQL);
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod(
                    "{\"column1\":\"value1\"}",
                    "{\"column2\":\"oldValue2\"}",
                    "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingUpdateJsonRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod(
                    "{\"column1\":\"value1\"}",
                    "{\"column2\":\"oldValue2\"}",
                    "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);

    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT664\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  /*
   * Change streams with NEW_ROW_AND_OLD_VALUES value capture type track both old values for
   * modified columns and the whole new row.
   */
  @Test
  public void testMappingUpdateStructRowNewRowAndOldValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod(
                    "{\"column1\":\"value1\"}",
                    "{\"column2\":\"oldValue2\"}",
                    "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingInsertStructRowNewRowAndOldValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", "{\"column2\":\"oldValue2\"}", null)),
            ModType.DELETE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\":\"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\":\"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteStructRowNewRowAndOldValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", "{\"column2\":\"oldValue2\"}", null)),
            ModType.DELETE,
            ValueCaptureType.NEW_ROW_AND_OLD_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct jsonFieldsStruct = recordsToStructWithJson(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(jsonFieldsStruct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UNKNOWN,
            ValueCaptureType.UNKNOWN,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final Struct struct = recordsWithUnknownModTypeAndValueCaptureType(dataChangeRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);

    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingStructRowToHeartbeatRecord() {
    final HeartbeatRecord heartbeatRecord =
        new HeartbeatRecord(Timestamp.ofTimeSecondsAndNanos(10L, 20), null);
    final Struct struct = recordsToStructWithStrings(heartbeatRecord);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);

    assertEquals(
        Collections.singletonList(heartbeatRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);

    assertEquals(
        Collections.singletonList(childPartitionsRecord),
        mapper.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
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
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);

    final PartitionMetadata initialPartition =
        partition.toBuilder().setPartitionToken(InitialPartition.PARTITION_TOKEN).build();

    assertEquals(
        Collections.singletonList(expected),
        mapper.toChangeStreamRecords(initialPartition, resultSet, resultSetMetadata));
  }

  /*
   * Change streams with NEW_ROW value capture type do not track old values, so null value
   * is used for OLD_VALUES_COLUMN in Mod.
   */
  @Test
  public void testMappingUpdateJsonRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  /*
   * Change streams with NEW_VALUES value capture type do not track old values, so null value
   * is used for OLD_VALUES_COLUMN in Mod.
   */
  @Test
  public void testMappingUpdateStructJsonNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "serverTransactionId",
            true,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingInsertJsonRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingInsertJsonRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingInsertJsonRowNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.INSERT,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteJsonRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", "{\"column2\":\"oldValue2\"}", null)),
            ModType.DELETE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteJsonRowNewRowToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\":\"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_ROW,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingDeleteJsonRowNewValuesToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(new Mod("{\"column1\":\"value1\"}", null, null)),
            ModType.DELETE,
            ValueCaptureType.NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingJsonRowWithUnknownModTypeAndValueCaptureTypeToDataChangeRecord() {
    final DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "transactionId",
            false,
            "1",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("{\"code\":\"INT64\"}"), true, 1L),
                new ColumnType("column2", new TypeCode("{\"code\":\"BYTES\"}"), false, 2L)),
            Collections.singletonList(
                new Mod("{\"column1\":\"value1\"}", null, "{\"column2\":\"newValue2\"}")),
            ModType.UNKNOWN,
            ValueCaptureType.UNKNOWN,
            10L,
            2L,
            "transactionTag",
            true,
            null);
    final String jsonString = recordToJson(dataChangeRecord, true, true);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(dataChangeRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingJsonRowToHeartbeatRecord() {
    final HeartbeatRecord heartbeatRecord =
        new HeartbeatRecord(Timestamp.ofTimeSecondsAndNanos(10L, 20), null);
    final String jsonString = recordToJson(heartbeatRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(heartbeatRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }

  @Test
  public void testMappingJsonRowToChildPartitionRecord() {
    final ChildPartitionsRecord childPartitionsRecord =
        new ChildPartitionsRecord(
            Timestamp.ofTimeSecondsAndNanos(10L, 20),
            "1",
            Arrays.asList(
                new ChildPartition("childToken1", Sets.newHashSet("parentToken1", "parentToken2")),
                new ChildPartition("childToken2", Sets.newHashSet("parentToken1", "parentToken2"))),
            null);
    final String jsonString = recordToJson(childPartitionsRecord, false, false);

    assertNotNull(jsonString);
    ChangeStreamResultSet resultSet = mock(ChangeStreamResultSet.class);
    when(resultSet.getPgJsonb(0)).thenReturn(jsonString);
    assertEquals(
        Collections.singletonList(childPartitionsRecord),
        mapperPostgres.toChangeStreamRecords(partition, resultSet, resultSetMetadata));
  }
}
