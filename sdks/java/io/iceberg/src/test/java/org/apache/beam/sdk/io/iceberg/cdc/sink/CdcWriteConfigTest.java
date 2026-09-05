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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.ValueKind;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CdcWriteConfig}. */
@RunWith(JUnit4.class)
public class CdcWriteConfigTest {

  private static final String SINK_ID = "test-sink-id";

  @Test
  public void builderAppliesDefaults() {
    CdcWriteConfig config = CdcWriteConfig.builder().setSinkId(SINK_ID).build();

    assertThat(
        config.getSequenceNumberColumn(), equalTo(CdcWriteConfig.DEFAULT_SEQUENCE_NUMBER_COLUMN));
    assertThat(config.getNumShards(), equalTo(CdcWriteConfig.DEFAULT_NUM_SHARDS));
    // Unset shards_per_partition resolves to num_shards: the resolved int carries no cap.
    assertThat(config.getShardsPerPartition(), equalTo(CdcWriteConfig.DEFAULT_NUM_SHARDS));
    assertThat(config.getSorterMemoryMB(), equalTo(CdcWriteConfig.DEFAULT_SORTER_MEMORY_MB));
    assertThat(config.getUpsert(), equalTo(false));
    assertThat(config.getTokenHeartbeatMillis(), nullValue());
    assertThat(config.getErrorHandling(), equalTo(false));
    assertThat(config.getEqualityColumns(), nullValue());
    assertThat(config.getChangeTypeColumn(), nullValue());
    assertThat(config.getChangeTypeMap(), nullValue());
    assertThat(config.getSnapshotProperties(), nullValue());
    assertThat(config.getSinkId(), equalTo(SINK_ID));
  }

  /** Both extremes of a legal config pass: everything optional unset, and everything set. */
  @Test
  public void validatePassesForDefaultsOnlyAndFullyPopulatedConfigs() {
    CdcWriteConfig.builder().setSinkId(SINK_ID).build().validate();
    fullyPopulatedBuilder().build().validate();
  }

  /** The config carries the resolved int as set; validation accepts the whole legal range. */
  @Test
  public void builderCarriesExplicitShardsPerPartition() {
    CdcWriteConfig config =
        CdcWriteConfig.builder()
            .setSinkId(SINK_ID)
            .setNumShards(16)
            .setShardsPerPartition(4)
            .build();

    assertThat(config.getShardsPerPartition(), equalTo(4));
    config.validate();
  }

  /** The whole rejection matrix: every field {@code validate()} bounds, one facet each. */
  @Test
  public void validateRejectsEachInvalidConfigField() {
    // facet: num_shards below one.
    CdcWriteConfig zeroShards = CdcWriteConfig.builder().setSinkId(SINK_ID).setNumShards(0).build();
    IllegalArgumentException numShards =
        assertThrows(IllegalArgumentException.class, () -> zeroShards.validate());
    assertThat(numShards.getMessage(), containsString("num_shards"));

    // facet: shards_per_partition below one.
    CdcWriteConfig sppZeroConfig =
        CdcWriteConfig.builder().setSinkId(SINK_ID).setShardsPerPartition(0).build();
    IllegalArgumentException sppZero =
        assertThrows(IllegalArgumentException.class, () -> sppZeroConfig.validate());
    assertThat(sppZero.getMessage(), containsString("shards_per_partition"));
    assertThat(sppZero.getMessage(), containsString("between 1 and num_shards"));

    // facet: shards_per_partition above num_shards.
    CdcWriteConfig sppAboveConfig =
        CdcWriteConfig.builder()
            .setSinkId(SINK_ID)
            .setNumShards(16)
            .setShardsPerPartition(32)
            .build();
    IllegalArgumentException sppAbove =
        assertThrows(IllegalArgumentException.class, () -> sppAboveConfig.validate());
    assertThat(sppAbove.getMessage(), containsString("shards_per_partition"));
    assertThat(sppAbove.getMessage(), containsString("32"));
    assertThat(sppAbove.getMessage(), containsString("16"));

    // facet: sorter_memory_mb below one.
    CdcWriteConfig sorterConfig =
        CdcWriteConfig.builder().setSinkId(SINK_ID).setSorterMemoryMB(0).build();
    IllegalArgumentException sorter =
        assertThrows(IllegalArgumentException.class, () -> sorterConfig.validate());
    assertThat(sorter.getMessage(), containsString("sorter_memory_mb"));

    // facet: explicitly empty equality_columns.
    CdcWriteConfig emptyEqConfig =
        CdcWriteConfig.builder()
            .setSinkId(SINK_ID)
            .setEqualityColumns(Collections.emptyList())
            .build();
    IllegalArgumentException emptyEq =
        assertThrows(IllegalArgumentException.class, () -> emptyEqConfig.validate());
    assertThat(emptyEq.getMessage(), containsString("equality_columns"));

    // facet: change-type column colliding with the sequence-number column.
    CdcWriteConfig collidingConfig =
        CdcWriteConfig.builder()
            .setSinkId(SINK_ID)
            .setSequenceNumberColumn("seq")
            .setChangeTypeColumn("seq")
            .build();
    IllegalArgumentException colliding =
        assertThrows(IllegalArgumentException.class, () -> collidingConfig.validate());
    assertThat(colliding.getMessage(), containsString("sequence_number_column"));
    assertThat(colliding.getMessage(), containsString("change_type_column"));

    // facet: change_type_map without a change_type_column.
    Map<String, String> orphanMap = new HashMap<>();
    orphanMap.put("c", "INSERT");
    CdcWriteConfig orphanMapConfig =
        CdcWriteConfig.builder().setSinkId(SINK_ID).setChangeTypeMap(orphanMap).build();
    IllegalArgumentException orphan =
        assertThrows(IllegalArgumentException.class, () -> orphanMapConfig.validate());
    assertThat(orphan.getMessage(), containsString("change_type_map"));
    assertThat(orphan.getMessage(), containsString("change_type_column"));

    // facet: a change_type_map value that is not a ValueKind name lists the legal names.
    Map<String, String> typoMap = new HashMap<>();
    typoMap.put("c", "INSSERT"); // typo: not a ValueKind name
    CdcWriteConfig typoMapConfig =
        CdcWriteConfig.builder()
            .setSinkId(SINK_ID)
            .setChangeTypeColumn("op")
            .setChangeTypeMap(typoMap)
            .build();
    IllegalArgumentException typo =
        assertThrows(IllegalArgumentException.class, () -> typoMapConfig.validate());
    assertThat(typo.getMessage(), containsString("change_type_map"));
    assertThat(typo.getMessage(), containsString("INSSERT"));
    assertThat(typo.getMessage(), containsString("INSERT"));
    assertThat(typo.getMessage(), containsString("UPDATE_BEFORE"));
    assertThat(typo.getMessage(), containsString("UPDATE_AFTER"));
    assertThat(typo.getMessage(), containsString("DELETE"));

    // facet: non-positive token heartbeat, named by the real option names.
    CdcWriteConfig heartbeatConfig =
        CdcWriteConfig.builder().setSinkId(SINK_ID).setTokenHeartbeatMillis(0L).build();
    IllegalArgumentException heartbeat =
        assertThrows(IllegalArgumentException.class, () -> heartbeatConfig.validate());
    assertThat(heartbeat.getMessage(), containsString("withTokenHeartbeat"));
    assertThat(heartbeat.getMessage(), containsString("token_heartbeat_seconds"));

    // facet: reserved beam.cdc. snapshot-property prefix.
    Map<String, String> reserved = new HashMap<>();
    reserved.put("beam.cdc.sink-id", "x");
    CdcWriteConfig reservedConfig =
        CdcWriteConfig.builder().setSinkId(SINK_ID).setSnapshotProperties(reserved).build();
    IllegalArgumentException reservedThrown =
        assertThrows(IllegalArgumentException.class, () -> reservedConfig.validate());
    assertThat(reservedThrown.getMessage(), containsString("snapshot_properties"));
    assertThat(reservedThrown.getMessage(), containsString("beam.cdc."));
  }

  @Test
  public void configIsJavaSerializable() {
    CdcWriteConfig config = fullyPopulatedBuilder().build();

    CdcWriteConfig deserialized = SerializableUtils.ensureSerializable(config);

    assertThat(deserialized, equalTo(config));
  }

  private static CdcWriteConfig.Builder fullyPopulatedBuilder() {
    Map<String, String> snapshotProperties = new HashMap<>();
    snapshotProperties.put("k", "v");

    return CdcWriteConfig.builder()
        .setSinkId(SINK_ID)
        .setEqualityColumns(Arrays.asList("id", "region"))
        .setSequenceNumberColumn("my_seq")
        .setChangeTypeColumn("op")
        .setChangeTypeMap(legalChangeTypeMap())
        .setNumShards(8)
        .setShardsPerPartition(1)
        .setSorterMemoryMB(200)
        .setUpsert(true)
        .setTokenHeartbeatMillis(60000L)
        .setSnapshotProperties(snapshotProperties)
        .setErrorHandling(true);
  }

  /** A {@link CdcWriteConfig#getChangeTypeMap()} value naming every {@link ValueKind} constant. */
  private static Map<String, String> legalChangeTypeMap() {
    Map<String, String> changeTypeMap = new HashMap<>();
    changeTypeMap.put("c", "INSERT");
    changeTypeMap.put("u", "UPDATE_AFTER");
    changeTypeMap.put("b", "UPDATE_BEFORE");
    changeTypeMap.put("d", "DELETE");
    return changeTypeMap;
  }
}
