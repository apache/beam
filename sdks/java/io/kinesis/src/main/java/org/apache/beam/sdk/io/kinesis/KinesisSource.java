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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents source for single stream in Kinesis. */
class KinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

  private final AWSClientsProvider awsClientsProvider;
  private final String streamName;
  private final Duration upToDateThreshold;
  private final WatermarkPolicyFactory watermarkPolicyFactory;
  private final RateLimitPolicyFactory rateLimitPolicyFactory;
  private CheckpointGenerator initialCheckpointGenerator;
  private final Integer limit;
  private final Integer maxCapacityPerShard;

  KinesisSource(
      AWSClientsProvider awsClientsProvider,
      String streamName,
      StartingPoint startingPoint,
      Duration upToDateThreshold,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RateLimitPolicyFactory rateLimitPolicyFactory,
      Integer limit,
      Integer maxCapacityPerShard) {
    this(
        awsClientsProvider,
        new DynamicCheckpointGenerator(streamName, startingPoint),
        streamName,
        upToDateThreshold,
        watermarkPolicyFactory,
        rateLimitPolicyFactory,
        limit,
        maxCapacityPerShard);
  }

  private KinesisSource(
      AWSClientsProvider awsClientsProvider,
      CheckpointGenerator initialCheckpoint,
      String streamName,
      Duration upToDateThreshold,
      WatermarkPolicyFactory watermarkPolicyFactory,
      RateLimitPolicyFactory rateLimitPolicyFactory,
      Integer limit,
      Integer maxCapacityPerShard) {
    this.awsClientsProvider = awsClientsProvider;
    this.initialCheckpointGenerator = initialCheckpoint;
    this.streamName = streamName;
    this.upToDateThreshold = upToDateThreshold;
    this.watermarkPolicyFactory = watermarkPolicyFactory;
    this.rateLimitPolicyFactory = rateLimitPolicyFactory;
    this.limit = limit;
    this.maxCapacityPerShard = maxCapacityPerShard;
    validate();
  }

  /**
   * Generate splits for reading from the stream. Basically, it'll try to evenly split set of shards
   * in the stream into {@code desiredNumSplits} partitions. Each partition is then a split.
   */
  @Override
  public List<KinesisSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    KinesisReaderCheckpoint checkpoint =
        initialCheckpointGenerator.generate(
            SimplifiedKinesisClient.from(awsClientsProvider, limit));

    List<KinesisSource> sources = newArrayList();

    for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
      sources.add(
          new KinesisSource(
              awsClientsProvider,
              new StaticCheckpointGenerator(partition),
              streamName,
              upToDateThreshold,
              watermarkPolicyFactory,
              rateLimitPolicyFactory,
              limit,
              maxCapacityPerShard));
    }
    return sources;
  }

  /**
   * Creates reader based on given {@link KinesisReaderCheckpoint}. If {@link
   * KinesisReaderCheckpoint} is not given, then we use {@code initialCheckpointGenerator} to
   * generate new checkpoint.
   */
  @Override
  public UnboundedReader<KinesisRecord> createReader(
      PipelineOptions options, KinesisReaderCheckpoint checkpointMark) {

    CheckpointGenerator checkpointGenerator = initialCheckpointGenerator;

    if (checkpointMark != null) {
      checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
    }

    LOG.info("Creating new reader using {}", checkpointGenerator);

    return new KinesisReader(
        SimplifiedKinesisClient.from(awsClientsProvider, limit),
        checkpointGenerator,
        this,
        watermarkPolicyFactory,
        rateLimitPolicyFactory,
        upToDateThreshold,
        maxCapacityPerShard);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public void validate() {
    checkNotNull(awsClientsProvider);
    checkNotNull(initialCheckpointGenerator);
    checkNotNull(watermarkPolicyFactory);
    checkNotNull(rateLimitPolicyFactory);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }

  String getStreamName() {
    return streamName;
  }
}
