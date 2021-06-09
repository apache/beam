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

import com.amazonaws.services.kinesis.model.Shard;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates {@link KinesisReaderCheckpoint}, which spans over all shards in given stream. List of
 * shards is obtained dynamically on call to {@link #generate(SimplifiedKinesisClient)}.
 */
class DynamicCheckpointGenerator implements CheckpointGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicCheckpointGenerator.class);
  private final String streamName;
  private final StartingPoint startingPoint;

  public DynamicCheckpointGenerator(String streamName, StartingPoint startingPoint) {
    this.streamName = streamName;
    this.startingPoint = startingPoint;
  }

  @Override
  public KinesisReaderCheckpoint generate(SimplifiedKinesisClient kinesis)
      throws TransientKinesisException {
    List<Shard> streamShards = kinesis.listShardsAtPoint(streamName, startingPoint);
    LOG.info(
        "Creating a checkpoint with following shards {} at {}",
        streamShards,
        startingPoint.getTimestamp());
    return new KinesisReaderCheckpoint(
        streamShards.stream()
            .map(shard -> new ShardCheckpoint(streamName, shard.getShardId(), startingPoint))
            .collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return String.format("Checkpoint generator for %s: %s", streamName, startingPoint);
  }
}
