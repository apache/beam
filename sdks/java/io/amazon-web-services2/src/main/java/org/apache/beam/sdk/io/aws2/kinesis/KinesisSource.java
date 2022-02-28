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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Read;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/** Represents source for single stream in Kinesis. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class KinesisSource extends UnboundedSource<KinesisRecord, KinesisReaderCheckpoint> {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

  private final Read spec;
  private final CheckpointGenerator checkpointGenerator;

  KinesisSource(Read read) {
    this(read, new DynamicCheckpointGenerator(read.getStreamName(), read.getInitialPosition()));
  }

  private KinesisSource(Read spec, CheckpointGenerator initialCheckpoint) {
    this.spec = checkNotNull(spec);
    this.checkpointGenerator = checkNotNull(initialCheckpoint);
  }

  private SimplifiedKinesisClient createClient(PipelineOptions options) {
    AwsOptions awsOptions = options.as(AwsOptions.class);
    KinesisClient kinesis;
    CloudWatchClient cloudWatch;
    if (spec.getAWSClientsProvider() != null) {
      kinesis = spec.getAWSClientsProvider().getKinesisClient();
      cloudWatch = spec.getAWSClientsProvider().getCloudWatchClient();
    } else {
      ClientConfiguration config = spec.getClientConfiguration();
      kinesis = ClientBuilderFactory.buildClient(awsOptions, KinesisClient.builder(), config);
      cloudWatch = ClientBuilderFactory.buildClient(awsOptions, CloudWatchClient.builder(), config);
    }
    return new SimplifiedKinesisClient(
        kinesis, cloudWatch, spec.getRequestRecordsLimit(), Instant::now);
  }

  /**
   * Generate splits for reading from the stream. Basically, it'll try to evenly split set of shards
   * in the stream into {@code desiredNumSplits} partitions. Each partition is then a split.
   */
  @Override
  public List<KinesisSource> split(int desiredNumSplits, PipelineOptions options) throws Exception {
    KinesisReaderCheckpoint checkpoint = checkpointGenerator.generate(createClient(options));
    List<KinesisSource> sources = newArrayList();
    for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
      sources.add(new KinesisSource(spec, new StaticCheckpointGenerator(partition)));
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

    CheckpointGenerator checkpointGenerator = this.checkpointGenerator;
    if (checkpointMark != null) {
      checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
    }

    LOG.info("Creating new reader using {}", checkpointGenerator);
    return new KinesisReader(spec, createClient(options), checkpointGenerator, this);
  }

  @Override
  public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
    return SerializableCoder.of(KinesisReaderCheckpoint.class);
  }

  @Override
  public Coder<KinesisRecord> getOutputCoder() {
    return KinesisRecordCoder.of();
  }
}
