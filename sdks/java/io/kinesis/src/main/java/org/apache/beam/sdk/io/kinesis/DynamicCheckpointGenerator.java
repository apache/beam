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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;

import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.base.Function;

/**
 * Creates {@link KinesisReaderCheckpoint}, which spans over all shards in given stream.
 * List of shards is obtained dynamically on call to {@link #generate(SimplifiedKinesisClient)}.
 */
class DynamicCheckpointGenerator implements CheckpointGenerator {

  private final String streamName;
  private final StartingPoint startingPoint;

  public DynamicCheckpointGenerator(String streamName, StartingPoint startingPoint) {
    this.streamName = checkNotNull(streamName, "streamName");
    this.startingPoint = checkNotNull(startingPoint, "startingPoint");
  }

  @Override
  public KinesisReaderCheckpoint generate(SimplifiedKinesisClient kinesis)
      throws TransientKinesisException {
    return new KinesisReaderCheckpoint(
        transform(kinesis.listShards(streamName), new Function<Shard, ShardCheckpoint>() {

          @Override
          public ShardCheckpoint apply(Shard shard) {
            return new ShardCheckpoint(streamName, shard.getShardId(), startingPoint);
          }
        })
    );
  }

  @Override
  public String toString() {
    return String.format("Checkpoint generator for %s: %s", streamName, startingPoint);
  }
}
