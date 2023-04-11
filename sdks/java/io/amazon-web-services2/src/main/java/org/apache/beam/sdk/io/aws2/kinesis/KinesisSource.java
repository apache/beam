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

import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Shard;

class KinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

  private final Read spec;
  private final @Nullable KinesisReaderCheckpoint initialCheckpoint;

  KinesisSource(Read read) {
    this(read, null);
  }

  KinesisSource(Read spec, @Nullable KinesisReaderCheckpoint initialCheckpoint) {
    this.spec = checkNotNull(spec);
    this.initialCheckpoint = initialCheckpoint;
  }

  /**
   * Generate splits for reading from the stream. Basically, it'll try to evenly split set of shards
   * in the stream into {@code desiredNumSplits} partitions. Each partition is then a split.
   */
  @Override
  public List<KinesisSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    List<KinesisSource> sources = new ArrayList<>();
    KinesisReaderCheckpoint checkpoint;

    // in case split() is called upon existing checkpoints for further splitting:
    if (this.initialCheckpoint != null) {
      checkpoint = this.initialCheckpoint;
    }
    // in case a new checkpoint is created from scratch:
    else {
      try (KinesisClient client = createKinesisClient(spec, options)) {
        checkpoint = generateInitCheckpoint(spec, client);
      }
    }

    for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
      sources.add(new KinesisSource(spec, partition));
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
      PipelineOptions options, @Nullable KinesisReaderCheckpoint checkpointMark)
      throws IOException {
    KinesisReaderCheckpoint initCheckpoint;
    if (checkpointMark != null) {
      LOG.info("Got checkpoint mark {}", checkpointMark);
      initCheckpoint = checkpointMark;
    } else {
      try {
        LOG.info("No checkpointMark specified, fall back to initial {}", this.initialCheckpoint);
        initCheckpoint = Preconditions.checkArgumentNotNull(this.initialCheckpoint);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return new KinesisReader(spec, createSimplifiedKinesisClient(options), initCheckpoint, this);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }

  KinesisClient createKinesisClient(Read spec, PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    if (spec.getAWSClientsProvider() != null) {
      return Preconditions.checkArgumentNotNull(spec.getAWSClientsProvider()).getKinesisClient();
    } else {
      ClientConfiguration config =
          Preconditions.checkArgumentNotNull(spec.getClientConfiguration());
      return buildClient(awsOptions, KinesisClient.builder(), config);
    }
  }

  private SimplifiedKinesisClient createSimplifiedKinesisClient(PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    Supplier<KinesisClient> kinesisSupplier = () -> createKinesisClient(spec, options);
    Supplier<CloudWatchClient> cloudWatchSupplier;
    AWSClientsProvider provider = spec.getAWSClientsProvider();
    if (provider != null) {
      cloudWatchSupplier = provider::getCloudWatchClient;
    } else {
      ClientConfiguration config =
          Preconditions.checkArgumentNotNull(spec.getClientConfiguration());
      cloudWatchSupplier = () -> buildClient(awsOptions, CloudWatchClient.builder(), config);
    }
    return new SimplifiedKinesisClient(
        kinesisSupplier, cloudWatchSupplier, spec.getRequestRecordsLimit());
  }

  /**
   * Creates a new checkpoint based on starting position.
   *
   * <p>This is needed only when persisted checkpointMark is not available for {@link
   * #createReader(PipelineOptions, KinesisReaderCheckpoint)}.
   */
  private KinesisReaderCheckpoint generateInitCheckpoint(Read spec, KinesisClient kinesis)
      throws IOException, InterruptedException {
    String stream = Preconditions.checkArgumentNotNull(spec.getStreamName());
    StartingPoint startingPoint = Preconditions.checkArgumentNotNull(spec.getInitialPosition());
    List<Shard> streamShards = ShardListingUtils.listShardsAtPoint(kinesis, stream, startingPoint);
    LOG.info("Creating a checkpoint with following shards {} at {}", streamShards, startingPoint);
    return new KinesisReaderCheckpoint(
        streamShards.stream()
            .map(shard -> new ShardCheckpoint(stream, shard.shardId(), startingPoint))
            .collect(Collectors.toList()));
  }
}
