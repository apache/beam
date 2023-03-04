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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisReaderCheckpoint;
import org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

/**
 * Always returns injected checkpoint. Used when stored checkpoint exists. TODO: add validation to
 * check if stored checkpoint is "rotten"?
 */
class StaticCheckpointGenerator implements CheckpointGenerator {

  private final KinesisReaderCheckpoint checkpoint;

  public StaticCheckpointGenerator(KinesisReaderCheckpoint checkpoint) {
    checkNotNull(checkpoint);
    List<ShardCheckpoint> result = new ArrayList<>();
    checkpoint.forEach(result::add);
    this.checkpoint = new KinesisReaderCheckpoint(result);
  }

  @Override
  public KinesisReaderCheckpoint generate(KinesisAsyncClient kinesis) {
    return checkpoint;
  }

  @Override
  public String toString() {
    return checkpoint.toString();
  }
}
