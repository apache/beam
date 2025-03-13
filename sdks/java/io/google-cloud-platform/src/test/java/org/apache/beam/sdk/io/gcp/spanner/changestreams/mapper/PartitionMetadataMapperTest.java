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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_CREATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_FINISHED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_RUNNING_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_SCHEDULED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_WATERMARK;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

public class PartitionMetadataMapperTest {

  private PartitionMetadataMapper mapper;

  @Before
  public void setUp() {
    mapper = new PartitionMetadataMapper();
  }

  @Test
  public void testMapPartitionMetadataFromResultSet() {
    final Struct row =
        Struct.newBuilder()
            .set(COLUMN_PARTITION_TOKEN)
            .to("token")
            .set(COLUMN_PARENT_TOKENS)
            .toStringArray(Collections.singletonList("parentToken"))
            .set(COLUMN_START_TIMESTAMP)
            .to(Timestamp.ofTimeMicroseconds(10L))
            .set(COLUMN_END_TIMESTAMP)
            .to(Timestamp.ofTimeMicroseconds(20L))
            .set(COLUMN_HEARTBEAT_MILLIS)
            .to(5_000L)
            .set(COLUMN_STATE)
            .to(State.RUNNING.name())
            .set(COLUMN_WATERMARK)
            .to(Timestamp.ofTimeMicroseconds(30L))
            .set(COLUMN_CREATED_AT)
            .to(Timestamp.ofTimeMicroseconds(40L))
            .set(COLUMN_SCHEDULED_AT)
            .to(Timestamp.ofTimeMicroseconds(50L))
            .set(COLUMN_RUNNING_AT)
            .to(Timestamp.ofTimeMicroseconds(60L))
            .set(COLUMN_FINISHED_AT)
            .to(Timestamp.ofTimeMicroseconds(70L))
            .build();

    final PartitionMetadata partition = mapper.from(row);

    assertEquals(
        new PartitionMetadata(
            "token",
            Sets.newHashSet("parentToken"),
            Timestamp.ofTimeMicroseconds(10L),
            Timestamp.ofTimeMicroseconds(20L),
            5_000L,
            State.RUNNING,
            Timestamp.ofTimeMicroseconds(30),
            Timestamp.ofTimeMicroseconds(40),
            Timestamp.ofTimeMicroseconds(50),
            Timestamp.ofTimeMicroseconds(60),
            Timestamp.ofTimeMicroseconds(70)),
        partition);
  }

  @Test
  public void testMapPartitionMetadataFromResultSetWithNulls() {
    final Struct row =
        Struct.newBuilder()
            .set(COLUMN_PARTITION_TOKEN)
            .to("token")
            .set(COLUMN_PARENT_TOKENS)
            .toStringArray(Collections.singletonList("parentToken"))
            .set(COLUMN_START_TIMESTAMP)
            .to(Timestamp.ofTimeMicroseconds(10L))
            .set(COLUMN_END_TIMESTAMP)
            .to(Timestamp.ofTimeMicroseconds(20L))
            .set(COLUMN_HEARTBEAT_MILLIS)
            .to(5_000L)
            .set(COLUMN_STATE)
            .to(State.CREATED.name())
            .set(COLUMN_WATERMARK)
            .to(Timestamp.ofTimeMicroseconds(30L))
            .set(COLUMN_CREATED_AT)
            .to(Timestamp.ofTimeMicroseconds(40L))
            .set(COLUMN_SCHEDULED_AT)
            .to((Timestamp) null)
            .set(COLUMN_RUNNING_AT)
            .to((Timestamp) null)
            .set(COLUMN_FINISHED_AT)
            .to((Timestamp) null)
            .build();

    final PartitionMetadata partition = mapper.from(row);

    assertEquals(
        new PartitionMetadata(
            "token",
            Sets.newHashSet("parentToken"),
            Timestamp.ofTimeMicroseconds(10L),
            Timestamp.ofTimeMicroseconds(20L),
            5_000L,
            State.CREATED,
            Timestamp.ofTimeMicroseconds(30),
            Timestamp.ofTimeMicroseconds(40),
            null,
            null,
            null),
        partition);
  }
}
