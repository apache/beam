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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.KinesisClientUtil;

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
      AwsOptions awsOptions = options.as(AwsOptions.class);
      ClientConfiguration config = spec.getClientConfiguration();
      try (KinesisClient client = buildClient(awsOptions, KinesisClient.builder(), config)) {
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
    return initReader(spec, options, initCheckpoint, this);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }

  /**
   * Provides final consumer ARN config.
   *
   * <p>{@link PipelineOptions} instance will overwrite anything given by {@link Read}.
   */
  static @Nullable String resolveConsumerArn(Read spec, PipelineOptions options) {
    String streamName = Preconditions.checkArgumentNotNull(spec.getStreamName());
    KinesisIOOptions sourceOptions = options.as(KinesisIOOptions.class);
    Map<String, String> streamToArnMapping = sourceOptions.getKinesisIOConsumerArns();

    String consumerArn;
    if (streamToArnMapping.containsKey(streamName)) {
      consumerArn = streamToArnMapping.get(streamName); // can resolve to null too
    } else {
      consumerArn = spec.getConsumerArn();
    }

    return consumerArn;
  }

  UnboundedReader<KinesisRecord> initReader(
      Read spec,
      PipelineOptions options,
      KinesisReaderCheckpoint checkpointMark,
      KinesisSource source) {
    String consumerArn = resolveConsumerArn(spec, options);

    if (consumerArn == null) {
      LOG.info("Creating new reader using {}", checkpointMark);
      return new KinesisReader(
          spec, createSimplifiedKinesisClient(options), checkpointMark, source);
    } else {
      LOG.info("Creating new EFO reader using {}", checkpointMark);
      return new EFOKinesisReader(
          spec, consumerArn, createAsyncClient(spec, options), checkpointMark, source);
    }
  }

  private SimplifiedKinesisClient createSimplifiedKinesisClient(PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    ClientConfiguration config = spec.getClientConfiguration();
    Supplier<KinesisClient> kinesisSupplier =
        () -> buildClient(awsOptions, KinesisClient.builder(), config);
    Supplier<CloudWatchClient> cloudWatchSupplier =
        () -> buildClient(awsOptions, CloudWatchClient.builder(), config);

    return new SimplifiedKinesisClient(
        kinesisSupplier, cloudWatchSupplier, spec.getRequestRecordsLimit());
  }

  private static KinesisAsyncClient createAsyncClient(Read spec, PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    KinesisAsyncClientBuilder builder =
        KinesisClientUtil.adjustKinesisClientBuilder(KinesisAsyncClient.builder());
    return buildClient(awsOptions, builder, spec.getClientConfiguration());
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
