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

import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

/**
 * Creates {@link KinesisReaderCheckpoint} when stored checkpoint is not available or outdated. List
 * of shards is obtained from Kinesis. The result of calling {@link #generate(KinesisAsyncClient)}
 * will depend on {@link StartingPoint} provided.
 */
class FromScratchCheckpointGenerator implements CheckpointGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(FromScratchCheckpointGenerator.class);
  private final KinesisIO.Read readSpec;

  FromScratchCheckpointGenerator(KinesisIO.Read readSpec) {
    this.readSpec = readSpec;
  }

  @Override
  public KinesisReaderCheckpoint generate(KinesisAsyncClient kinesis)
      throws TransientKinesisException {
    List<ShardCheckpoint> streamShards =
        ShardsListingUtils.generateShardsCheckpoints(readSpec, kinesis);

    LOG.info(
        "Creating a checkpoint with following shards {} at {}",
        streamShards,
        readSpec.getInitialPosition());
    return new KinesisReaderCheckpoint(streamShards);
  }

  @Override
  public String toString() {
    return String.format(
        "Checkpoint generator for %s: %s", readSpec.getStreamName(), readSpec.getInitialPosition());
  }
}
