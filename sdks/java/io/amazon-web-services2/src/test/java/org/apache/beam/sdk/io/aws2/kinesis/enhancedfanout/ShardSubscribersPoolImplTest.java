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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.Helpers.createConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.Helpers;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientProxyStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisStubBehaviours;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class ShardSubscribersPoolImplTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws TransientKinesisException, IOException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecords();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = Helpers.waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeSeqNumber("shard-000", "6"),
            Helpers.subscribeSeqNumber("shard-001", "12"),
            Helpers.subscribeSeqNumber("shard-000", "18"),
            Helpers.subscribeSeqNumber("shard-001", "24"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolHandlesShardUp() throws TransientKinesisException, IOException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecordsAndShardUp();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = Helpers.waitForRecords(pool, 70);
    assertEquals(70, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeTrimHorizon("shard-002"),
            Helpers.subscribeTrimHorizon("shard-003"),
            Helpers.subscribeTrimHorizon("shard-004"),
            Helpers.subscribeTrimHorizon("shard-005"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolHandlesShardDown() throws TransientKinesisException, IOException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.fourShardsWithRecordsAndShardDown();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = Helpers.waitForRecords(pool, 77);

    assertEquals(77, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeLatest("shard-002"),
            Helpers.subscribeLatest("shard-003"),
            Helpers.subscribeTrimHorizon("shard-004"),
            Helpers.subscribeTrimHorizon("shard-005"),
            Helpers.subscribeTrimHorizon("shard-006"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolReSubscribesUponRecoverableError() throws TransientKinesisException, IOException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis =
        KinesisStubBehaviours.twoShardsWithRecordsOneShardRecoverableError();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    List<KinesisRecord> actualRecords = Helpers.waitForRecords(pool, 20);
    assertEquals(20, actualRecords.size());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeSeqNumber("shard-000", "10"),
            Helpers.subscribeSeqNumber("shard-001", "20"),
            Helpers.subscribeSeqNumber("shard-000", "30"),
            Helpers.subscribeSeqNumber("shard-001", "40"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    assertTrue(pool.stop());
  }

  @Test
  public void poolPropagatesCriticalError() throws TransientKinesisException {
    Config config = createConfig();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecordsOneShardError();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);
    ShardSubscribersPoolImpl pool =
        new ShardSubscribersPoolImpl(config, kinesis, initialCheckpoint);
    assertTrue(pool.start());
    Exception e = assertThrows(IOException.class, () -> Helpers.waitForRecords(pool, 20));
    Throwable nestedError = e.getCause().getCause();
    assertTrue(nestedError instanceof RuntimeException);
    assertEquals("Oh..", nestedError.getMessage());
    List<SubscribeToShardRequest> expectedSubscribeRequests =
        ImmutableList.of(
            Helpers.subscribeLatest("shard-000"),
            Helpers.subscribeLatest("shard-001"),
            Helpers.subscribeSeqNumber("shard-000", "10"));
    assertTrue(kinesis.subscribeRequestsSeen().containsAll(expectedSubscribeRequests));
    // check that there's only one subscribe request for error-ed shard
    long shard01Reconnects =
        kinesis.subscribeRequestsSeen().stream()
            .filter(r -> r.shardId().equals("shard-001"))
            .count();
    assertEquals(1L, shard01Reconnects);
  }
}
