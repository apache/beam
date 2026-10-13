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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class InitializeDoFnTest {

  private DaoFactory daoFactory;
  private MapperFactory mapperFactory;
  private OutputReceiver<PartitionMetadata> receiver;
  private PartitionMetadataDao partitionMetadataDao;
  private PartitionMetadataAdminDao partitionMetadataAdminDao;
  private PartitionMetadataMapper partitionMetadataMapper;
  private InitializeDoFn initializeDoFn;

  @Before
  public void setUp() {
    daoFactory = mock(DaoFactory.class);
    mapperFactory = mock(MapperFactory.class);
    receiver = mock(OutputReceiver.class);
    partitionMetadataDao = mock(PartitionMetadataDao.class);
    partitionMetadataAdminDao = mock(PartitionMetadataAdminDao.class);
    partitionMetadataMapper = mock(PartitionMetadataMapper.class);
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
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(partitionMetadataDao.insert(any(PartitionMetadata.class)))
        .thenReturn(Timestamp.ofTimeMicroseconds(1L));
    when(partitionMetadataDao.getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, ChangeStreamsConstants.DEFAULT_TVF_NAME)))
        .thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));
    initializeDoFn.processElement(receiver);
    verify(daoFactory, times(2)).getPartitionMetadataDao();
    verify(daoFactory, times(1)).getPartitionMetadataAdminDao();
    verify(partitionMetadataDao, times(1)).insert(any(PartitionMetadata.class));
    verify(partitionMetadataDao, times(1))
        .getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, ChangeStreamsConstants.DEFAULT_TVF_NAME));
    verify(partitionMetadataDao, times(1)).tableExists();
    verify(mapperFactory, times(1)).partitionMetadataMapper();
    verify(partitionMetadataMapper, times(1)).from(any());
  }

  @Test
  public void testInitializeWithNoPartition() {
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    when(partitionMetadataDao.insert(any(PartitionMetadata.class)))
        .thenReturn(Timestamp.ofTimeMicroseconds(1L));
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
    when(partitionMetadataDao.insert(listCaptor.capture()))
        .thenReturn(Timestamp.ofTimeMicroseconds(1L));
    when(partitionMetadataDao.getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, "tvf1")))
        .thenReturn(Struct.newBuilder().build());
    when(mapperFactory.partitionMetadataMapper()).thenReturn(partitionMetadataMapper);
    when(partitionMetadataMapper.from(any())).thenReturn(mock(PartitionMetadata.class));

    initializeDoFn.processElement(receiver);

    verify(daoFactory, times(2)).getPartitionMetadataDao();
    verify(daoFactory, times(1)).getPartitionMetadataAdminDao();

    assertEquals(2, listCaptor.getValue().size());
    assertEquals("tvf1", listCaptor.getValue().get(0).getTvfName());
    assertEquals("tvf2", listCaptor.getValue().get(1).getTvfName());
    verify(partitionMetadataDao, times(1)).insert(listCaptor.capture());
    verify(partitionMetadataDao, times(1))
        .getPartition(
            PartitionMetadataDao.composePartitionTokenWithTvfName(
                InitialPartition.PARTITION_TOKEN, "tvf1"));
    verify(partitionMetadataDao, times(1)).tableExists();
    verify(mapperFactory, times(1)).partitionMetadataMapper();
    verify(partitionMetadataMapper, times(1)).from(any());
  }

  @Test
  public void testInitializeWithNoPartitionWithTvfNameList() {
    when(daoFactory.getTvfNameList()).thenReturn(Arrays.asList("tvf1", "tvf2"));
    when(daoFactory.getPartitionMetadataDao()).thenReturn(partitionMetadataDao);
    when(partitionMetadataDao.tableExists()).thenReturn(false);
    when(daoFactory.getPartitionMetadataAdminDao()).thenReturn(partitionMetadataAdminDao);
    doNothing().when(partitionMetadataAdminDao).createPartitionMetadataTable();
    ArgumentCaptor<List<PartitionMetadata>> listCaptor = ArgumentCaptor.forClass(List.class);
    when(partitionMetadataDao.insert(listCaptor.capture()))
        .thenReturn(Timestamp.ofTimeMicroseconds(1L));
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
