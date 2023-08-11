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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Value;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PartitionMetadataDaoTest {

  private DatabaseClient databaseClient;

  private TransactionRunner readWriteTransactionRunner;

  private TransactionContext transaction;

  private PartitionMetadataDao.InTransactionContext inTransactionContext;
  private PartitionMetadataDao partitionMetadataDao;
  private static final String METADATA_TABLE_NAME = "SPANNER_TABLE";
  private static final String PARTITION_TOKEN = "partitionToken123";
  private static final String PARENT_TOKEN = "parentToken123";
  private static final Timestamp START_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(1, 1);
  private static final Timestamp END_TIMESTAMP = Timestamp.ofTimeSecondsAndNanos(2, 2);
  private static final Timestamp WATERMARK = Timestamp.ofTimeSecondsAndNanos(3, 3);
  private static final Timestamp CREATED_AT = Timestamp.ofTimeSecondsAndNanos(4, 4);
  private static final Timestamp SCHEDULED_AT = Timestamp.ofTimeSecondsAndNanos(5, 5);
  private static final Timestamp RUNNING_AT = Timestamp.ofTimeSecondsAndNanos(6, 6);
  private static final Timestamp FINISHED_AT = Timestamp.ofTimeSecondsAndNanos(7, 7);

  private static final PartitionMetadata ROW =
      PartitionMetadata.newBuilder()
          .setPartitionToken(PARTITION_TOKEN)
          .setParentTokens(Sets.newHashSet(PARENT_TOKEN))
          .setStartTimestamp(START_TIMESTAMP)
          .setEndTimestamp(END_TIMESTAMP)
          .setHeartbeatMillis(10)
          .setState(PartitionMetadata.State.RUNNING)
          .setWatermark(WATERMARK)
          .setCreatedAt(CREATED_AT)
          .setScheduledAt(SCHEDULED_AT)
          .setRunningAt(RUNNING_AT)
          .setFinishedAt(FINISHED_AT)
          .build();

  @Before
  public void setUp() {
    databaseClient = mock(DatabaseClient.class);
    partitionMetadataDao =
        new PartitionMetadataDao(METADATA_TABLE_NAME, databaseClient, Dialect.GOOGLE_STANDARD_SQL);
    readWriteTransactionRunner = mock(TransactionRunner.class);
    transaction = mock(TransactionContext.class);
    inTransactionContext =
        new PartitionMetadataDao.InTransactionContext(
            METADATA_TABLE_NAME, transaction, Dialect.GOOGLE_STANDARD_SQL);
  }

  @Test
  public void testInsert() {
    when(databaseClient.readWriteTransaction(anyObject())).thenReturn(readWriteTransactionRunner);
    when(databaseClient.readWriteTransaction()).thenReturn(readWriteTransactionRunner);
    when(readWriteTransactionRunner.run(any())).thenReturn(null);
    when(readWriteTransactionRunner.getCommitTimestamp())
        .thenReturn(Timestamp.ofTimeMicroseconds(1L));
    Timestamp commitTimestamp = partitionMetadataDao.insert(ROW);
    verify(databaseClient, times(1)).readWriteTransaction(anyObject());
    verify(readWriteTransactionRunner, times(1)).run(any());
    verify(readWriteTransactionRunner, times(1)).getCommitTimestamp();
    assertEquals(Timestamp.ofTimeMicroseconds(1L), commitTimestamp);
  }

  @Test
  public void testInTransactionContextInsert() {
    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.insert(ROW));
    assertEquals(1, mutations.getValue().size());
    Map<String, Value> mutationValueMap = mutations.getValue().iterator().next().asMap();
    assertEquals(
        ROW.getPartitionToken(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN).getString());
    assertEquals(
        ImmutableList.of(PARENT_TOKEN),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS).getStringArray());
    assertEquals(
        ROW.getStartTimestamp(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP).getTimestamp());
    assertEquals(
        ROW.getEndTimestamp(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP).getTimestamp());
    assertEquals(
        ROW.getHeartbeatMillis(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS).getInt64());
    assertEquals(
        ROW.getState().toString(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_STATE).getString());
    assertEquals(
        ROW.getWatermark(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_WATERMARK).getTimestamp());
  }

  @Test
  public void testInTransactionContextCannotUpdateToRunning() {
    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.updateToRunning(PARTITION_TOKEN));
    // assertEquals(0, mutations.getValue().size());
    verify(transaction, times(0)).buffer(mutations.capture());
  }

  @Test
  public void testInTransactionContextUpdateToRunning() {
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(any())).thenReturn(State.SCHEDULED.toString());
    when(resultSet.getCurrentRowAsStruct()).thenReturn(Struct.newBuilder().build());

    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.updateToRunning(PARTITION_TOKEN));
    assertEquals(1, mutations.getValue().size());
    Map<String, Value> mutationValueMap = mutations.getValue().iterator().next().asMap();
    assertEquals(
        PARTITION_TOKEN,
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN).getString());
    assertEquals(
        PartitionMetadata.State.RUNNING.toString(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_STATE).getString());
  }

  @Test
  public void testInTransactionContextCannotUpdateToScheduled() {
    System.out.println("Cannot update to scheduled");
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.updateToScheduled(Collections.singletonList(PARTITION_TOKEN)));
    verify(transaction, times(0)).buffer(mutations.capture());
  }

  @Test
  public void testInTransactionContextUpdateToScheduled() {
    System.out.println(" update to scheduled");
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString(any())).thenReturn(PARTITION_TOKEN);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(Struct.newBuilder().build());

    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.updateToScheduled(Collections.singletonList(PARTITION_TOKEN)));
    assertEquals(1, mutations.getValue().size());
    Map<String, Value> mutationValueMap = mutations.getValue().iterator().next().asMap();
    assertEquals(
        PARTITION_TOKEN,
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN).getString());
    assertEquals(
        PartitionMetadata.State.SCHEDULED.toString(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_STATE).getString());
  }

  @Test
  public void testInTransactionContextUpdateToFinished() {
    System.out.println("update to scheduled");
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getString(any())).thenReturn(State.RUNNING.toString());
    when(resultSet.getCurrentRowAsStruct()).thenReturn(Struct.newBuilder().build());

    ArgumentCaptor<ImmutableList<Mutation>> mutations =
        ArgumentCaptor.forClass(ImmutableList.class);
    doNothing().when(transaction).buffer(mutations.capture());
    assertNull(inTransactionContext.updateToFinished(PARTITION_TOKEN));
    assertEquals(1, mutations.getValue().size());
    Map<String, Value> mutationValueMap = mutations.getValue().iterator().next().asMap();
    assertEquals(
        PARTITION_TOKEN,
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN).getString());
    assertEquals(
        PartitionMetadata.State.FINISHED.toString(),
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_STATE).getString());
  }

  @Test
  public void testInTransactionContextUpdateWatermark() {
    ArgumentCaptor<Mutation> mutation = ArgumentCaptor.forClass(Mutation.class);
    doNothing().when(transaction).buffer(mutation.capture());
    assertNull(inTransactionContext.updateWatermark(PARTITION_TOKEN, WATERMARK));
    Map<String, Value> mutationValueMap = mutation.getValue().asMap();
    assertEquals(
        PARTITION_TOKEN,
        mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN).getString());
    assertEquals(
        WATERMARK, mutationValueMap.get(PartitionMetadataAdminDao.COLUMN_WATERMARK).getTimestamp());
  }

  @Test
  public void testInTransactionContextGetPartitionWithNoPartitions() {
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    assertNull(inTransactionContext.getPartition(PARTITION_TOKEN));
  }

  @Test
  public void testInTransactionContextGetPartitionWithPartitions() {
    ResultSet resultSet = mock(ResultSet.class);
    when(transaction.executeQuery(any(), anyObject())).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(Struct.newBuilder().build());
    assertNotNull(inTransactionContext.getPartition(PARTITION_TOKEN));
  }
}
