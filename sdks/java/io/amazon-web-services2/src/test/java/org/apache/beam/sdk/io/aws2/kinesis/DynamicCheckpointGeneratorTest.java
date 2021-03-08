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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.InitialPositionInStream;

/** * */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Shard.class)
public class DynamicCheckpointGeneratorTest {

  @Mock private SimplifiedKinesisClient kinesisClient;
  @Mock private StartingPointShardsFinder startingPointShardsFinder;
  @Mock private Shard shard1, shard2, shard3;

  @Test
  public void shouldMapAllShardsToCheckpoints() throws Exception {
    when(shard1.shardId()).thenReturn("shard-01");
    when(shard2.shardId()).thenReturn("shard-02");
    when(shard3.shardId()).thenReturn("shard-03");
    Set<Shard> shards = Sets.newHashSet(shard1, shard2, shard3);
    StartingPoint startingPoint = new StartingPoint(InitialPositionInStream.LATEST);
    when(startingPointShardsFinder.findShardsAtStartingPoint(
            kinesisClient, "stream", startingPoint))
        .thenReturn(shards);
    DynamicCheckpointGenerator underTest =
        new DynamicCheckpointGenerator("stream", startingPoint, startingPointShardsFinder);

    KinesisReaderCheckpoint checkpoint = underTest.generate(kinesisClient);

    assertThat(checkpoint).hasSize(3);
  }

  @Test
  public void shouldMapAllValidShardsToCheckpoints() throws Exception {
    when(shard1.shardId()).thenReturn("shard-01");
    when(shard2.shardId()).thenReturn("shard-02");
    when(shard3.shardId()).thenReturn("shard-03");
    String streamName = "stream";
    Set<Shard> shards = Sets.newHashSet(shard1, shard2);
    StartingPoint startingPoint = new StartingPoint(InitialPositionInStream.LATEST);
    when(startingPointShardsFinder.findShardsAtStartingPoint(
            kinesisClient, "stream", startingPoint))
        .thenReturn(shards);

    DynamicCheckpointGenerator underTest =
        new DynamicCheckpointGenerator(streamName, startingPoint, startingPointShardsFinder);

    KinesisReaderCheckpoint checkpoint = underTest.generate(kinesisClient);
    assertThat(checkpoint)
        .hasSize(2)
        .doesNotContain(new ShardCheckpoint(streamName, shard3.shardId(), startingPoint));
  }
}
