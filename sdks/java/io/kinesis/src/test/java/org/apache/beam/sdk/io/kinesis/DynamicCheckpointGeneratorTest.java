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
package org.apache.beam.sdk.io.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Shard;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** * */
@RunWith(MockitoJUnitRunner.class)
public class DynamicCheckpointGeneratorTest {

  @Mock private SimplifiedKinesisClient kinesisClient;
  @Mock private Shard shard1, shard2, shard3;

  @Test
  public void shouldMapAllShardsToCheckpoints() throws Exception {
    when(shard1.getShardId()).thenReturn("shard-01");
    when(shard2.getShardId()).thenReturn("shard-02");
    when(shard3.getShardId()).thenReturn("shard-03");
    List<Shard> shards = ImmutableList.of(shard1, shard2, shard3);
    String streamName = "stream";
    StartingPoint startingPoint = new StartingPoint(InitialPositionInStream.LATEST);
    when(kinesisClient.listShardsAtPoint(streamName, startingPoint)).thenReturn(shards);
    DynamicCheckpointGenerator underTest =
        new DynamicCheckpointGenerator(streamName, startingPoint);

    KinesisReaderCheckpoint checkpoint = underTest.generate(kinesisClient);

    assertThat(checkpoint).hasSize(3);
  }
}
