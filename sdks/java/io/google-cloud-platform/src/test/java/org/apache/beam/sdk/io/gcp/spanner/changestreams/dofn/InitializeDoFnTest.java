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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao.InTransactionContext;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TestTransactionAnswer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class InitializeDoFnTest {

  private DaoFactory daoFactory;
  private MapperFactory mapperFactory;
  private OutputReceiver<PartitionMetadata> receiver;
  private PartitionMetadataDao partitionMetadataDao;
  private InTransactionContext transaction;
  private PartitionMetadataAdminDao partitionMetadataAdminDao;
  private PartitionMetadataMapper partitionMetadataMapper;
  private InitializeDoFn initializeDoFn;

  @Before
  public void setUp() {
    daoFactory = mock(DaoFactory.class);
    mapperFactory = mock(MapperFactory.class);
    receiver = mock(OutputReceiver.class);
    partitionMetadataDao = mock(PartitionMetadataDao.class);
    transaction = mock(InTransactionContext.class);
    partitionMetadataAdminDao = mock(PartitionMetadataAdminDao.class);
    partitionMetadataMapper = mock(PartitionMetadataMapper.class);
    when(partitionMetadataDao.runInTransaction(any(), any()))
        .thenAnswer(new TestTransactionAnswer(transaction));
    initializeDoFn =
        new InitializeDoFn(
            daoFactory,
            mapperFactory,
            Timestamp.ofTimeMicroseconds(1L),
            Timestamp.ofTimeMicroseconds(2L),
            2000L);
  }

  @Test
  public void testInitialize() {
    String composedToken =
        PartitionMetadataDao.composePartitionTokenWithTvfName(
            InitialPartition.PARTITION_TOKEN, ChangeStreamsConstants.DEFAULT_TVF_NAME);
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(transaction.getPartition(composedToken)).thenReturn(null);
    ArgumentCaptor<List<PartitionMetadata>> listCaptor = ArgumentCaptor.forClass(List.class);
    when(partitionMetadataDao.getPartition(composedToken)).thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    PartitionMetadata expectedPartition = mock(PartitionMetadata.class);
    when(partitionMetadataMapper.from(any())).thenReturn(expectedPartition);

    initializeDoFn.processElement(receiver);

    verify(daoFactory, times(2)).getPartitionMetadataDao();
    verify(daoFactory, times(1)).getPartitionMetadataAdminDao();
    verify(transaction, times(1)).getPartition(composedToken);
    verify(transaction, times(1)).insert(listCaptor.capture());
    assertEquals(1, listCaptor.getValue().size());
    assertEquals(
        InitialPartition.PARTITION_TOKEN, listCaptor.getValue().get(0).getPartitionToken());
    verify(partitionMetadataDao, times(1)).getPartition(composedToken);
    verify(partitionMetadataDao, times(1)).tableExists();
    verify(mapperFactory, times(1)).partitionMetadataMapper();
    verify(partitionMetadataMapper, times(1)).from(any());
    verify(receiver, times(1)).output(expectedPartition);
  }

  @Test
  public void testInitializeWhenFakeParentPartitionAlreadyExists() {
    String composedToken =
        PartitionMetadataDao.composePartitionTokenWithTvfName(
            InitialPartition.PARTITION_TOKEN, ChangeStreamsConstants.DEFAULT_TVF_NAME);
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(transaction.getPartition(composedToken)).thenReturn(Struct.newBuilder().build());
    when(partitionMetadataDao.getPartition(composedToken)).thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    PartitionMetadata expectedPartition = mock(PartitionMetadata.class);
    when(partitionMetadataMapper.from(any())).thenReturn(expectedPartition);

    initializeDoFn.processElement(receiver);

    verify(transaction, times(1)).getPartition(composedToken);
    verify(transaction, never()).insert(any(List.class));
    verify(transaction, never()).insert(any(PartitionMetadata.class));
    verify(receiver, times(1)).output(expectedPartition);
  }

  @Test
  public void testInitializeIgnoresAlreadyExistsSpannerException() {
    String composedToken =
        PartitionMetadataDao.composePartitionTokenWithTvfName(
            InitialPartition.PARTITION_TOKEN, ChangeStreamsConstants.DEFAULT_TVF_NAME);
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    doThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.ALREADY_EXISTS, "Row [Parent0] already exists"))
        .when(partitionMetadataDao)
        .runInTransaction(any(), any());
    when(partitionMetadataDao.getPartition(composedToken)).thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    PartitionMetadata expectedPartition = mock(PartitionMetadata.class);
    when(partitionMetadataMapper.from(any())).thenReturn(expectedPartition);

    initializeDoFn.processElement(receiver);

    verify(receiver, times(1)).output(expectedPartition);
  }

  @Test
  public void testInitializeRethrowsNonAlreadyExistsSpannerException() {
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    doThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.PERMISSION_DENIED, "Permission denied"))
        .when(partitionMetadataDao)
        .runInTransaction(any(), any());

    try {
      initializeDoFn.processElement(receiver);
      fail();
    } catch (SpannerException e) {
      assertEquals(ErrorCode.PERMISSION_DENIED, e.getErrorCode());
    }
  }

  @Test
  public void testInitializeWithNoPartition() {
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));
    try {
      initializeDoFn.processElement(receiver);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Initial partition not found in metadata table.", e.getMessage());
    }
  }

  @Test
  public void testInitializeWithTvfNameList() {
    when(daoFactory.getTvfNameList()).thenReturn(Arrays.asList("tvf1", "tvf2"));
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    ArgumentCaptor<List<PartitionMetadata>> listCaptor = ArgumentCaptor.forClass(List.class);
    when(partitionMetadataDao.getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, "tvf1")))
        .thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));

    initializeDoFn.processElement(receiver);

    verify(daoFactory, times(2)).getPartitionMetadataDao();
    verify(daoFactory, times(1)).getPartitionMetadataAdminDao();
    verify(transaction, times(1)).insert(listCaptor.capture());

    assertEquals(2, listCaptor.getValue().size());
    assertEquals("tvf1", listCaptor.getValue().get(0).getTvfName());
    assertEquals("tvf2", listCaptor.getValue().get(1).getTvfName());
    verify(partitionMetadataDao, times(1))
        .getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, "tvf1"));
    verify(partitionMetadataDao, times(1)).tableExists();
    verify(mapperFactory, times(1)).partitionMetadataMapper();
    verify(partitionMetadataMapper, times(1)).from(any());
  }

  @Test
  public void testInitializeWithTvfNameListWhenSomeAlreadyExist() {
    String composedToken1 =
        PartitionMetadataDao.composePartitionTokenWithTvfName(
            InitialPartition.PARTITION_TOKEN, "tvf1");
    String composedToken2 =
        PartitionMetadataDao.composePartitionTokenWithTvfName(
            InitialPartition.PARTITION_TOKEN, "tvf2");
    when(daoFactory.getTvfNameList()).thenReturn(Arrays.asList("tvf1", "tvf2"));
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(transaction.getPartition(composedToken1)).thenReturn(Struct.newBuilder().build());
    when(transaction.getPartition(composedToken2)).thenReturn(null);
    ArgumentCaptor<List<PartitionMetadata>> listCaptor = ArgumentCaptor.forClass(List.class);
    when(partitionMetadataDao.getPartition(composedToken1)).thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));

    initializeDoFn.processElement(receiver);

    verify(transaction, times(1)).insert(listCaptor.capture());
    assertEquals(1, listCaptor.getValue().size());
    assertEquals("tvf2", listCaptor.getValue().get(0).getTvfName());
  }

  @Test
  public void testInitializeWithNoPartitionWithTvfNameList() {
    when(daoFactory.getTvfNameList()).thenReturn(Arrays.asList("tvf1", "tvf2"));
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));

    try {
      initializeDoFn.processElement(receiver);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Initial partition not found in metadata table.", e.getMessage());
    }
  }
}
