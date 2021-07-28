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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_CREATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_END_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_FINISHED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_HEARTBEAT_MILLIS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_INCLUSIVE_END;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_INCLUSIVE_START;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_PARENT_TOKENS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_PARTITION_TOKEN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_RUNNING_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_SCHEDULED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_START_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.COLUMN_STATE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

public class PartitionMetadataMapperTest {

  private PartitionMetadataMapper mapper;
  private ResultSet resultSet;

  @Before
  public void setUp() {
    mapper = new PartitionMetadataMapper();
    resultSet = mock(ResultSet.class);
  }

  @Test
  public void testMapPartitionMetadataFromResultSet() {
    when(resultSet.getString(COLUMN_PARTITION_TOKEN)).thenReturn("token");
    when(resultSet.getStringList(COLUMN_PARENT_TOKENS))
        .thenReturn(Collections.singletonList("parentToken"));
    when(resultSet.getTimestamp(COLUMN_START_TIMESTAMP))
        .thenReturn(Timestamp.ofTimeMicroseconds(10));
    when(resultSet.getBoolean(COLUMN_INCLUSIVE_START)).thenReturn(true);
    when(resultSet.getTimestamp(COLUMN_END_TIMESTAMP)).thenReturn(Timestamp.ofTimeMicroseconds(20));
    when(resultSet.getBoolean(COLUMN_INCLUSIVE_END)).thenReturn(true);
    when(resultSet.getLong(COLUMN_HEARTBEAT_MILLIS)).thenReturn(5_000L);
    when(resultSet.getString(COLUMN_STATE)).thenReturn("FINISHED");
    when(resultSet.getTimestamp(COLUMN_CREATED_AT)).thenReturn(Timestamp.ofTimeMicroseconds(30));
    when(resultSet.getTimestamp(COLUMN_SCHEDULED_AT)).thenReturn(Timestamp.ofTimeMicroseconds(40));
    when(resultSet.getTimestamp(COLUMN_RUNNING_AT)).thenReturn(Timestamp.ofTimeMicroseconds(50));
    when(resultSet.getTimestamp(COLUMN_FINISHED_AT)).thenReturn(Timestamp.ofTimeMicroseconds(60));

    final PartitionMetadata partition = mapper.from(resultSet);

    assertEquals(
        new PartitionMetadata(
            "token",
            Sets.newHashSet("parentToken"),
            Timestamp.ofTimeMicroseconds(10L),
            true,
            Timestamp.ofTimeMicroseconds(20L),
            true,
            5_000L,
            State.FINISHED,
            Timestamp.ofTimeMicroseconds(30),
            Timestamp.ofTimeMicroseconds(40),
            Timestamp.ofTimeMicroseconds(50),
            Timestamp.ofTimeMicroseconds(60)),
        partition);
  }

  @Test
  public void testMapPartitionMetadataFromResultSetWithNulls() {
    when(resultSet.getString(COLUMN_PARTITION_TOKEN)).thenReturn("token");
    when(resultSet.getStringList(COLUMN_PARENT_TOKENS))
        .thenReturn(Collections.singletonList("parentToken"));
    when(resultSet.getTimestamp(COLUMN_START_TIMESTAMP))
        .thenReturn(Timestamp.ofTimeMicroseconds(10));
    when(resultSet.getBoolean(COLUMN_INCLUSIVE_START)).thenReturn(true);
    when(resultSet.getLong(COLUMN_HEARTBEAT_MILLIS)).thenReturn(5_000L);
    when(resultSet.getString(COLUMN_STATE)).thenReturn("CREATED");
    when(resultSet.getTimestamp(COLUMN_CREATED_AT)).thenReturn(Timestamp.ofTimeMicroseconds(30));

    final PartitionMetadata partition = mapper.from(resultSet);

    assertEquals(
        new PartitionMetadata(
            "token",
            Sets.newHashSet("parentToken"),
            Timestamp.ofTimeMicroseconds(10L),
            true,
            null,
            false,
            5_000L,
            State.CREATED,
            Timestamp.ofTimeMicroseconds(30),
            null,
            null,
            null),
        partition);
  }
}
